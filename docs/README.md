Welcome to the GraphFrames Spark Package documentation!

This readme will walk you through navigating and building the GraphFrames documentation, which is included here with the source code.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the documentation yourself. Why build it yourself? So that you have the docs that correspond to whichever version of GraphFrames you currently have checked out of revision control.

## Generating the Documentation HTML

We include the GraphFrames documentation as part of the source (as opposed to using a hosted wiki, such as the github wiki, as the definitive documentation) to enable the documentation to evolve along with the source code and be captured by revision control (currently git). This way the code automatically
includes the version of the documentation that is relevant regardless of which version or release you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can read those text files directly if you want. Start with index.md.

The markdown code can be compiled to HTML using the [Jekyll tool](http://jekyllrb.com).
`Jekyll` and a few dependencies must be installed for this to work. We recommend installing via the Ruby Gem dependency manager. Since the exact HTML output varies between versions of Jekyll and its dependencies, we list specific versions here in some cases:

    $ gem install jekyll
    $ gem install jekyll-redirect-from

On macOS, with the default Ruby, please install Jekyll with Bundler as [instructed on offical website](https://jekyllrb.com/docs/quickstart/). Otherwise the build script might fail to resolve dependencies.

    $ gem install jekyll bundler
    $ gem install jekyll-redirect-from

Execute `jekyll build` from the `docs/` directory to compile the site. Compiling the site with Jekyll will create a directory called `_site` containing index.html as well as the rest of the compiled files.

You can modify the default Jekyll build as follows:

    # Skip generating API docs (which takes a while)
    $ SKIP_API=1 jekyll build
    # Serve content locally on port 4000
    $ jekyll serve --watch
    # Build the site with extra features used on the live page
    $ PRODUCTION=1 jekyll build

Note that `SPARK_HOME` must be set to your local Spark installation in order to generate the docs.

To manually point to a specific `Spark` installation,
    $ SPARK_HOME=<your-path-to-spark-home> PRODUCTION=1 jekyll build

## Sphinx

We use Sphinx to generate Python API docs, so you will need to install it by running (once we upgrade to Python 3.10 it will get added to the dev requirements):

    pip install sphinx

## API Docs (Scaladoc, Sphinx)

You can build just the scaladoc by running `build/sbt unidoc` from the GRAPHFRAMES_PROJECT_ROOT directory.

Similarly, you can build just the Python docs by running `make html` from the GRAPHFRAMES_PROJECT_ROOT/python/docs directory. Documentation is only generated for classes that are listed as public in `__init__.py`.

When you run `jekyll` in the `docs` directory, it will also copy over the scaladoc for the various subprojects into the `docs` directory (and then also into the `_site` directory). We use a jekyll plugin to run `build/sbt unidoc` before building the site so if you haven't run it (recently) it may take some time as it generates all of the scaladoc.  The jekyll plugin also generates the
Python docs [Sphinx](http://sphinx-doc.org/).

NOTE: To skip the step of building and copying over the Scala, Python API docs, run `SKIP_API=1 jekyll build`. To skip building Scala API docs, run `SKIP_SCALADOC=1 jekyll build`; to skip building Python API docs, run `SKIP_PYTHONDOC=1 jekyll build`.
