"""Utilities to prepare data for fitting a model or running a meta-analysis."""
import abc
import contextlib
import json
import logging
from pathlib import Path
import tempfile
from typing import Optional, Dict

import numpy as np
from scipy import sparse
import pandas as pd

from nqdc import _img_utils
from nqdc._typing import NiftiMasker

_LOG = logging.getLogger(__name__)


class DataManager(abc.ABC):
    """Helper class to load data and fit an encoding model.

    It helps to:
    - load the data
    - compute the article brain maps
    - reindex metadata, tfidf and brain maps so they are indexed by the same
      pmcids
    - drop the terms that are too rare

    Clients should inherit from this class and redefine `_fit_model`.

    In `_fit_model`, we can rely on the public attributes of this class such as
    tfidf and brain_maps; they have been loaded and aligned on their pmcids.

    """

    _MIN_DOCUMENT_FREQUENCY = 10
    _BRAIN_MAP_DTYPE = "float32"
    _VOXEL_SIZE = 4.0

    def __init__(
        self,
        tfidf_dir: Path,
        extracted_data_dir: Path,
        n_jobs: int,
    ) -> None:
        self.tfidf_dir = tfidf_dir
        self.extracted_data_dir = extracted_data_dir
        self.n_jobs = n_jobs
        self.metadata: Optional[pd.DataFrame] = None
        self.tfidf: Optional[sparse.csr_matrix] = None
        self.coordinates: Optional[pd.DataFrame] = None
        self.brain_maps: Optional[np.memmap] = None
        self._brain_maps_pmcids: Optional[np.ndarray] = None
        self.full_voc: Optional[pd.DataFrame] = None
        self.voc_mapping: Optional[Dict[str, str]] = None
        self.feature_names: Optional[pd.DataFrame] = None
        self.masker: Optional[NiftiMasker] = None
        self.context: Optional[contextlib.ExitStack] = None

    @staticmethod
    def _img_filter(
        coordinates: pd.DataFrame,
        masker: NiftiMasker,
        output: np.memmap,
        idx: int,
    ) -> None:
        """"""
        _img_utils._gaussian_coords_to_masked_map(
            coordinates, masker, output, idx
        )

    @abc.abstractmethod
    def _fit_model(self) -> None:
        """Do the actual model fitting."""

    def _load_tfidf(self) -> None:
        self.tfidf = sparse.load_npz(
            str(self.tfidf_dir.joinpath("merged_tfidf.npz"))
        )
        self.feature_names = pd.read_csv(
            self.tfidf_dir.joinpath("feature_names.csv"), header=None
        )
        self.full_voc = pd.read_csv(
            self.tfidf_dir.joinpath("vocabulary.csv"), header=None
        )
        self.voc_mapping = json.loads(
            self.tfidf_dir.joinpath(
                "vocabulary.csv_voc_mapping_identity.json"
            ).read_text("utf-8")
        )

    def _load_data(self) -> None:
        self.metadata = pd.read_csv(
            self.extracted_data_dir.joinpath("metadata.csv")
        )
        self.coordinates = pd.read_csv(
            self.extracted_data_dir.joinpath("coordinates.csv")
        )
        self._load_tfidf()

    def _compute_brain_maps(self) -> None:
        assert self.context is not None

        tmp_dir = self.context.enter_context(tempfile.TemporaryDirectory())
        memmap_file = str(Path(tmp_dir).joinpath("brain_maps.dat"))
        _LOG.debug("Computing article maps.")
        target_affine = (self._VOXEL_SIZE, self._VOXEL_SIZE, self._VOXEL_SIZE)
        brain_maps, pmcids, masker = _img_utils._coordinates_to_memmapped_maps(
            coordinates=self.coordinates,
            output_memmap_file=memmap_file,
            output_dtype=self._BRAIN_MAP_DTYPE,
            img_filter=self._img_filter,
            target_affine=target_affine,
            n_jobs=self.n_jobs,
            context=self.context,
        )
        _LOG.debug("Done computing article maps.")
        non_empty = (brain_maps != 0).any(axis=1)
        self._brain_maps_pmcids = pmcids[non_empty]
        self.brain_maps = brain_maps[non_empty]
        self.context.callback(self._reset_brain_maps)
        self.masker = masker

    def _reset_brain_maps(self) -> None:
        # when exiting the context the brainmaps memmap is closed; we rest
        # brain_maps here so that we don't keep a reference to a closed memmap
        self.brain_maps = None

    def _set_pmcids(self) -> None:
        """Reindex metadata, tfidf and brain maps.

        After this, their rows match (correspond to the same pmcids).
        """
        assert self.metadata is not None
        assert self.brain_maps is not None
        assert self._brain_maps_pmcids is not None
        assert self.tfidf is not None

        tfidf_pmcids = self.metadata["pmcid"].values
        pmcids = np.asarray(
            sorted(set(self._brain_maps_pmcids).intersection(tfidf_pmcids))
        )
        maps_rindex = pd.Series(
            np.arange(len(self._brain_maps_pmcids)),
            index=self._brain_maps_pmcids,
        )
        self.brain_maps = self.brain_maps[maps_rindex.loc[pmcids].values, :]

        rindex = pd.Series(np.arange(len(tfidf_pmcids)), index=tfidf_pmcids)
        self.tfidf = sparse.csr_matrix(
            self.tfidf.A[rindex.loc[pmcids].values, :]
        )

        self.metadata.set_index("pmcid", inplace=True)
        # false positive: pylint thinks read_csv returns a TextFileReader
        # pylint: disable-next=no-member
        self.metadata = self.metadata.loc[pmcids, :]
        # pylint: disable-next=no-member
        self.metadata.index.name = "pmcid"
        # pylint: disable-next=no-member
        self.metadata.reset_index(inplace=True)

    def _filter_out_rare_terms(self) -> None:
        """Drop very rare terms, update tfidf and vocabulary."""
        assert self.tfidf is not None
        assert self.full_voc is not None
        assert self.feature_names is not None
        assert self.voc_mapping is not None

        kept = np.asarray(
            (self.tfidf > 0).sum(axis=0) > self._MIN_DOCUMENT_FREQUENCY
        ).ravel()
        self.tfidf = self.tfidf[:, kept]
        self.feature_names = self.feature_names[kept]
        feat_names_set = set(self.feature_names.iloc[:, 0].values)
        self.voc_mapping = {
            source: target
            for (source, target) in self.voc_mapping.items()
            if target in feat_names_set
        }
        voc_set = feat_names_set.union(self.voc_mapping.keys())
        self.full_voc = self.full_voc[self.full_voc.iloc[:, 0].isin(voc_set)]

    def fit(self) -> None:
        """Load data and fit the model or meta-analysis."""
        with contextlib.ExitStack() as self.context:
            self._load_data()
            self._compute_brain_maps()
            self._set_pmcids()
            self._filter_out_rare_terms()
            self._fit_model()
