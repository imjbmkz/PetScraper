## PetScraper

### Prerequisites

Install `uv` for package management.

#### Linux/MacOS
```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### Windows
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Install `MySQL` for the database. Follow this [guide](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/). 

### Installation

Run the following command to sync environment with project dependecies:
```sh
uv sync
```

Activate the virtual environment.
```sh
source .venv/bin/activate
```

### Setup

Copy and configure the .env file.

```sh
cp .env-sample .env
```

### Usage

```sh
python3 scraper.py <task> [-s <shop>]
```

#### Arguments
- `task` (required): Identify the task to be executed.
  - `get_links`: Get links from registered shops.
  - `scrape`: Scrape products.
- `-s`, `--shop` (optional): Select a shop to scrape. Choices are:
  - `Zooplus`
  - `PetsAtHome`
  - `PetPlanet`
  
  If not provided, all shops will be scraped by default.

