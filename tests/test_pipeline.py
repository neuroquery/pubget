from unittest.mock import Mock

from pubget import _pipeline, _typing


def test_stop_pipeline():
    all_steps = Mock(), Mock(), Mock()
    for step in all_steps:
        step.run.return_value = None, 0
    all_steps[1].run.side_effect = _typing.StopPipeline("fatal error")
    pipeline = _pipeline.Pipeline(all_steps)
    pipeline.run(Mock())
    for step in all_steps[:2]:
        assert step.run.called
    assert not all_steps[2].called
