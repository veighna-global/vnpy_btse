# BTSE trading gateway for VeighNa Evo

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-2024.5.8-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.10|3.11|3.12-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## Introduction

This gateway is developed based on BTSE's REST and Websocket API, and supports spot, futures contract and swap contract trading.

For derivatives contract trading, please notice:

1. Only supports cross margin mode.
2. Only supports one-way position mode.

## Install

Users can easily install ``vnpy_btse`` by pip according to the following command.

```
pip install vnpy_btse
```

Also, users can install ``vnpy_btse`` using the source code. Clone the repository and install as follows:

```
git clone https://github.com/veighna-global/vnpy_btse.git && cd vnpy_btse

python setup.py install
```

## A Simple Example

Save this as run.py.

```
from vnpy_evo.event import EventEngine
from vnpy_evo.trader.engine import MainEngine
from vnpy_evo.trader.ui import MainWindow, create_qapp

from vnpy_btse import BtseGateway


def main():
    """Start VeighNa Trader"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BtseGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
```
