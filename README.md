# railgun_privacy

Scripts for retrieving and analysing datasets related to Railgun privacy.

## Prerequisites
- Python 3.10+
- pip

## Installation
```bash
git clone https://github.com/<your-username>/railgun_privacy.git
cd railgun_privacy
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

## Environment
```bash
cp .env.example .env
```
Edit `.env` to add your Alchemy API key:
```
ALCHEMY_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY
```

## Usage
See the top comments in the relevant scripts for details:
- `dataset/scripts/retriever.py`
- `dataset/scripts/analyser.py`