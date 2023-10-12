from textual.app import App, ComposeResult
from textual.widgets import Header, Footer
from clickhouse_driver import Client
from textual_plotext import PlotextPlot
from textual.screen import Screen
from textual.containers import Grid
import datetime


client = Client(host='localhost')


class BasePlot(PlotextPlot):
    def __init__(
        self,
        title: str,
        *,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
        disabled: bool = False,
        unit: str | None = None
    ) -> None:
        super().__init__(name=name, id=id, classes=classes, disabled=disabled)
        self._title = title
        self._unit = unit
        self._data: list[float] = []
        self._time: list[int | str] = []

    def on_mount(self) -> None:
        self.plt.date_form("Y-m-d H:M:S")
        self.plt.title(self._title)
        self.plt.xlabel("Time")
        self.set_interval(1, self.update, pause=False)

    def replot(self) -> None:
        self.plt.clear_data()
        self.plt.ylabel(self._unit)
        self.plt.plot(self._time, self._data)
        self.refresh()

    def update(self):
        pass


class MemoryConsumptionPlot(BasePlot):
    def update(self) -> None:
        cur_queries = client.execute(
            "SELECT sum(memory_usage) FROM system.query_log WHERE event_time >= date_sub(second, 1, now())")[0][0]
        self._data.append(cur_queries)
        if len(self._data) > 50:
            self._data = self._data[1:]
        self._time.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.replot()


class QueryAmountPlot(BasePlot):
    def update(self) -> None:
        cur_queries = client.execute(
             "SELECT count(*) FROM system.query_log WHERE event_time >= date_sub(second, 1, now())")[0][0]
        self._data.append(cur_queries)
        if len(self._data) > 50:
            self._data = self._data[1:]
        self._time.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.replot()


class ErrorsAmountPlot(BasePlot):
    def update(self) -> None:
        cur_queries = client.execute(
             "SELECT count(*) FROM system.query_log WHERE event_time >= date_sub(second, 1, now()) AND system.query_log.exception != ''")[0][0]
        self._data.append(cur_queries)
        if len(self._data) > 50:
            self._data = self._data[1:]
        self._time.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.replot()


class SendReceivedBytes(BasePlot):
    def __init__(
        self,
        title: str,
        *,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
        disabled: bool = False,
        unit: str | None = None,
    ) -> None:
        super().__init__(title, name=name, id=id, classes=classes, disabled=disabled)
        self._title = title
        self._unit = unit
        self._data_1: list[float] = []
        self._data_2: list[float] = []
        self._time: list[int | str] = []

    def update(self) -> None:
        cur_queries = client.execute(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NetworkSendBytes_eth0'")[0][0]
        self._data_1.append(cur_queries)
        if len(self._data_1) > 50:
            self._data_1 = self._data_1[1:]
        cur_queries = client.execute(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NetworkReceiveBytes_eth0'")[0][0]
        self._data_2.append(cur_queries)
        if len(self._data_2) > 50:
            self._data_2 = self._data_2[1:]
        self._time.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.replot()

    def replot(self) -> None:
        self.plt.clear_data()
        self.plt.ylabel(self._unit)
        self.plt.plot(self._time, self._data_1, label="Send")
        self.plt.plot(self._time, self._data_2, label="Received")
        self.refresh()


class MainScreen(Screen):
    def compose(self) -> ComposeResult:
        yield Header()
        with Grid():
            yield MemoryConsumptionPlot("Memory consumption", unit="Bytes")
            yield QueryAmountPlot("Running queries amount")
            yield SendReceivedBytes("Send and received bytes via network", unit="Bytes")
            yield ErrorsAmountPlot("Errors amount")
        yield Footer()


class CHTopApp(App):
    CSS = """
    Grid {
        grid-size: 2;
    }
    """

    def on_mount(self) -> None:
        self.install_screen(MainScreen(), name="main")
        self.push_screen("main")


if __name__ == "__main__":
    app = CHTopApp()
    app.run()