# target-odoo-v3

`target-odoo-v3` is a Singer target for OdooV3.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install target-odoo-v3
```

## Configuration

| Name | Required | Description | Example |
|---|---|---|---|
| `url` | Yes | Base URL of your Odoo instance. | `https://mycompany.odoo.com` |
| `db` | Yes | Name of the Odoo database to connect to. | `mycompany` |
| `username` | Yes | Odoo login username (usually an email address). | `admin@mycompany.com` |
| `password` | Yes | Odoo login password or API key. | `xxxxxxxxxxxxxxxxx` |
| `export_buy_orders_as_draft` | No | When `true`, purchase orders are created in `draft` state instead of `purchase`. Defaults to `false`. | `true` |
| `verify_ref` | No | When `true`, invoices and bills whose reference already exists in Odoo are skipped. Defaults to `false`. | `true` |
| `input_path` | No | Directory where attachment files are looked up before uploading to Odoo. Defaults to `./`. | `/data/attachments` |

```json
{
  "url": "https://mycompany.odoo.com",
  "db": "mycompany",
  "username": "admin@mycompany.com",
  "password": "xxxxxxxxxxxxxxxxx"
}
```

## Usage

You can easily run `target-odoo-v3` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-odoo-v3 --version
target-odoo-v3 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-odoo-v3 --config /path/to/target-odoo-v3-config.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_odoo_v3/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-odoo-v3` CLI interface directly using `poetry run`:

```bash
poetry run target-odoo-v3 --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-odoo-v3
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-odoo-v3 --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-odoo-v3
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
