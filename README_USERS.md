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

This project makes use of Pipenv. If you are new to pipenv, install it and
study the output of ``pipenv --help``, especially the commands ``pipenv lock``
and ``pipenv sync``. Or read the [docs](https://docs.pipenv.org/).

First, clone this repo and make some required directories.

```shell
git clone https://github.com/pyprogrammerblog/tiny-blocks.git
cd tiny-blocks
```

Then build the docker image, providing your user and group ids for correct file
permissions.

```shell
docker-compose build
```

Then run the app and access inside the docker.

```shell
docker-compose run --rm app bash
(docker)
```

We create a Pipenv virtual environment, adding the `--site-packages` switch
to be able to import python packages that you installed with apt inside the
docker.

```shell
pipenv --site-packages
```

If you want to bump package versions, regenerate the `Pipfile.lock`.

```shell
pipenv lock
```

Then install the packages (including dev packages) listed in `Pipfile.lock`.

```shell
pipenv sync --dev
```
Then exit the docker shell (Ctrl + D)
At this point, you may want to test your installation.

```shell
docker-compose run --rm app pipenv run pytest --cov=smart_stream
```
Or start working with the tiny-blocks right away.

```shell
docker-compose up
```

To stop all running containers without removing them, do this.

```shell
docker-compose stop
```

### Jupyter Notebook

Hit the command

```shell
docker-compose run --rm -p 8888:8888 app pipenv shell
```

Then inside the docker:

```shell
jupyter notebook --ip 0.0.0.0 --no-browser --allow-root
```

That is all!

Happy Coding!
