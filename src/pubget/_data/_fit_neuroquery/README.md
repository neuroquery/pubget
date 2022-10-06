# NeuroQuery encoding model fitted on data fetched by pubget

To interactively query the model, run from inside this directory:

```
pip install -r requirements.txt
flask run
```

Then open the adress shown by flask (by default it should be
http://127.0.0.1:5000/) in your web browser.
See the Flask documentation for details.

To use the model in a Python script, install the `neuroquery` package and load
it from the `neuroquery_model` directory with:

```
from neuroquery import NeuroQueryModel
model = NeuroQueryModel.from_data_dir("neuroquery_model")
```

See the neuroquery documentation for details.
