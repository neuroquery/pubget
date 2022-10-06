# Example pubget plugin

This is a plugin that does not do much (it plots the number of downloaded
articles per publication year) to illustrate how to write a plugin and make it
discoverable by pubget, and to be used as a template.

Plugins are discovered through the `pubget.plugin_actions` entry point
(see the [setuptools documentation on entry
points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins)).

It is defined in the `get_pubget_actions` function (see
`src/pubget_example_plugin/__init__.py`), which is referenced in the
`[options.entry_points]` section of `setup.cfg`. This allows our plugin to be
invoked through the `pubget` command and run as if it were a part of `pubget`
itself.
