# Example nqdc plugin

This is a plugin that does not do much (it plots the number of downloaded
articles per publication year) to illustrate how to write a plugin and make it
discoverable by nqdc, and to be used as a template.

Plugins are discovered through the `nqdc.plugin_actions` entry point
(see the [setuptools documentation on entry
points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins)).

It is defined in the `get_nqdc_actions` function (see
`src/nqdc_example_plugin/__init__.py`), which is referenced in the
`[options.entry_points]` section of `setup.cfg`. This allows our plugin to be
invoked through the `nqdc` command and run as if it were a part of `nqdc`
itself.
