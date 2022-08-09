 tiny-blocks
=============

Clone the project and move inside:
```shell
git clone https://github.com/pyprogrammerblog/tiny-blocks.git
cd tiny-blocks
```
 
Install the virtualenv on the root project folder:
```shell
docker-compose run --rm --no-deps app poetry install
```

Check your installed dependencies for security vulnerabilities
```shell
docker-compose run --rm app poetry check
```

Run the tests:
```shell
docker-compose run --rm app poetry run pytest
```

Shut down all services
```shell
docker-compose down
```

### Local development

These instructions assume that ``git``, ``docker``, and ``docker-compose`` are
installed on your host machine.

First, clone this repo and make some required directories.

```shell
git clone https://github.com/pyprogrammerblog/tiny-blocks.git
cd tiny-blocks
```

Then build the docker image

```shell
docker-compose build app
```

Install packages

```shell
docker-compose run --rm app poetry install
```

Run test suite

```shell
docker-compose run --rm app poetry run pytest
```

You can access to the shell
```shell
docker-compose run --rm app poetry shell
```

To stop all running containers without removing them, do this.

```shell
docker-compose stop
```

### Jupyter Notebook

Hit the command

```shell
docker-compose run --rm -p 8888:8888 app poetry shell
```

Then inside the docker:

```shell
jupyter notebook --ip 0.0.0.0 --no-browser --allow-root
```

That is all!

Happy Coding!
