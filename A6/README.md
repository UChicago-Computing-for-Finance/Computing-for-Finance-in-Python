# FINM32500-A6
Design Patterns in Financial Software Architecture

This small project demonstrates several design patterns applied to simple financial instruments and analytics.

## Setup

- Python 3.8+ (project was developed with Python 3.12 in this environment).
- Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

## Project layout (key modules)

- `patterns/Factory_InstrumentTypes.py` — Instrument base class and `InstrumentFactory` that creates `Stock`, `Bond`, and `ETF` instances from dictionaries or CSV rows.
- `patterns/Singleton_ConfigAccess.py` — Simple singleton `Config` class that loads `inputs/config.json` so all modules share the same configuration instance.
- `Decorator_Analytics.py` — Decorator-based analytics implementations. Contains:
	- `InstrumentDecorator` (base wrapper)
	- `VolatilityDecorator` (computes std of simple returns from `inputs/market_data.csv`)
	- `BetaDecorator` (computes beta vs a market proxy, default `SPY`)
	- `DrawdownDecorator` (computes maximum drawdown)
- `demo_decorators.py` — Small demo script showing how to stack decorators:

```python
decorated = DrawdownDecorator(BetaDecorator(VolatilityDecorator(stock)))
print(decorated.get_metrics())
```

- `tests/test_patterns.py` — Unit tests validating:
	- Factory creation (Stock/Bond/ETF)
	- Singleton `Config` behavior
	- Decorator-enhanced analytics outputs (volatility, beta, drawdown)
	- Observer notifications (`LoggerObserver`, `AlertObserver`)
	- Command execute/undo logic via the engine's Command pattern
	- Strategy outputs for `MeanReversionStrategy` and `BreakoutStrategy`

## Data / Inputs

- `inputs/market_data.csv` — sample price ticks used by the analytics decorators.
- `inputs/config.json` — configuration used by the singleton `Config` class.

## Run the demo

You can run the notebook `PatternDemo.ipynb` or the small demo script that shows decorator stacking:

```bash
python3 demo_decorators.py
```
or open and run the cells in `PatternDemo.ipynb`.


## Run the unit tests

From the project root run either:

```bash
python3 -m unittest tests.test_patterns -v
```

or run discovery for all tests:

```bash
python3 -m unittest discover -v
```
