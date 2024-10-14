# Unreleased

## Features

* #79 Added function get_cli_arg that makes a string CLI argument from an option and its value.
  Also allowed passing an option name instead of the StdParams in the following two functions:
  create_std_option, check_params.

## Bug fixing

* #78 Missing default value in the definition of StdParams.path_in_bucket.
