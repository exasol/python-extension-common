# 0.8.0 - 2024-10-14

## Features

* #52: Added timeout options for SLC deployer to CLI
* #79 Added function `get_cli_arg` that makes a string CLI argument from an option and its value.
  Also allowed passing an option name instead of the `StdParams` in the following two functions:
  `create_std_option`, `check_params`.

## Bug fixing

* #78 Missing default value in the definition of `StdParams.path_in_bucket`.

## Documentation

* #81 Updated the documentation on the CLI commands, following the introduction of the standard
  CLI parameters.
