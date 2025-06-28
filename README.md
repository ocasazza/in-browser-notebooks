# JupyterLite Demo

[![lite-badge](https://jupyterlite.rtfd.io/en/latest/_static/badge.svg)](https://jupyterlite.github.io/demo)

JupyterLite deployed as a static site to GitHub Pages, for demo purposes.

Navigate to [the demo ticket analysis dashboard](https://ocasazza.github.io/in-browser-notebooks/lab/index.html?path=pyodide%2Fticket-analysis.ipynb) for an interactive example.

## Requirements

JupyterLite is being tested against modern web browsers:

- Firefox 90+
- Chromium 89+

## Local Development

setup a virtual environment

```sh
virtualenv venv && source venv/bin/activate && pip install -r requirements.txt
```

start a development server

```sh
jupyter lite serve --contents content/
```

## Further Information and Updates

For more info, keep an eye on the JupyterLite documentation:

- How-to Guides: https://jupyterlite.readthedocs.io/en/latest/howto/index.html
- Reference: https://jupyterlite.readthedocs.io/en/latest/reference/index.html

This template provides the Pyodide kernel (`jupyterlite-pyodide-kernel`), the JavaScript kernel (`jupyterlite-javascript-kernel`), and the p5 kernel (`jupyterlite-p5-kernel`), along with other
optional utilities and extensions to make the JupyterLite experience more enjoyable. See the
[`requirements.txt` file](requirements.txt) for a list of all the dependencies provided.

For a template based on the Xeus kernel, see the [`jupyterlite/xeus-python-demo` repository](https://github.com/jupyterlite/xeus-python-demo)
