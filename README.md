# Uporabniki


## Configuration

Configuration is done in 2 layers (3 are planned). First the application loads configuration
parameters from the `config.json` file. Then if any of the MACROs are also in the shell environment,
for example in Bash: `export DATABASE_IP="some_ip"` the parameter from the config file gets overridden.

## Run app

	docker-compose up

## Run tests

While app is running, you can invoke unittests by running:

	pipenv run python api_test.py
# Ocene
