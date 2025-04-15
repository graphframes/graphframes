Welcome to the GraphFrames Spark Package documentation!

# Build and deploy documentation

Documentation of GraphFrames package is build automatically during the CI.

# Build the documentation locally

## Build Python API docs

From the `./python` directory run the following:

```sh
poetry run bash -c "cd docs && make clean && make html"
```

## Build core documentation

From the root of the project:

```sh
gem install jekyll jekyll-redicrect-from bundler
cd docs
jukyll build
```
