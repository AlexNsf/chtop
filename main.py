from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Button, DataTable
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
            yield Button("All running queries")
        yield Footer()


class AllRunningQueries(DataTable):
    BINDINGS = [
        ("escape", "app.pop_screen", "Pop screen"),
    ]

    def on_mount(self) -> None:
        self.add_column("id", key="id")
        self.add_column("Memory usage", key="memory_usage")
        self.add_column("Elapsed (total time of execution)", key="elapsed")
        self.set_interval(1, self.update, pause=False)

    def update(self):
        present_query_ids = set()

        for query_id, memory_usage, elapsed in client.execute("SELECT query_id, memory_usage, elapsed FROM system.processes"):
            present_query_ids.add(query_id)
            row = {"query_id": query_id, "memory_usage": memory_usage, "elapsed": elapsed}

            if not self.rows.get(query_id):
                self.add_row(query_id, memory_usage, elapsed, key=query_id)
            else:
                for column_key, _ in self.columns:
                    if self.get_cell(row_key=query_id, column_key=column_key) != row[column_key]:
                        self.update_cell(row_key=query_id, column_key=column_key, value=row[column_key])

        ids_to_delete = set(self.rows.keys()) - present_query_ids
        for query_id in ids_to_delete:
            self.remove_row(query_id)


class AllRunningQueriesScreen(Screen):
    def compose(self) -> ComposeResult:
        yield AllRunningQueries()


class CHTopApp(App):
    CSS = """
    Grid {
        grid-size: 2;
    }
    """

    def on_button_pressed(self):
        self.push_screen("all_queries")

    def on_mount(self) -> None:
        self.install_screen(MainScreen(), name="main")
        self.install_screen(AllRunningQueriesScreen(), name="all_queries")
        self.push_screen("main")


if __name__ == "__main__":
    app = CHTopApp()
    app.run()
