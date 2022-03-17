from concurrent.futures import process
from dataclasses import dataclass, field
from multiprocessing import Event
from operator import eq
from functools import partial
import os
from sys import getsizeof
import time
import psutil

from tabulate import tabulate
from base.DataFormatter import DataFormatter, EventTypeClassifier
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from stream.Stream import OutputStream, Stream
from stream.FileStream import FileInputStream, FileOutputStream
from CEP import CEP
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable, SimpleCondition
from misc.ConsumptionPolicy import ConsumptionPolicy
from misc.SelectionStrategies import SelectionStrategies
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from adaptive.statistics.StatisticsCollectorFactory import StatisticsCollectorParameters
from adaptive.optimizer.OptimizerFactory import StatisticsDeviationAwareOptimizerParameters, InvariantsAwareOptimizerParameters
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from datetime import timedelta, datetime

import gc
import timeit
import itertools as it

from operator import itemgetter

from test.testUtils import runBenchMark


@dataclass
class Condition:
    field: str
    relation_op: callable


@dataclass
class EventPattern:
    event_type: str
    var_name: str
    conditions: list[Condition] = field(default_factory=list)

    def get_conditions(self):
        return [
            SimpleCondition(
                Variable(self.var_name, itemgetter(condition.field)),
                relation_op=condition.relation_op
            )
            for condition in self.conditions
        ]

def get_size_rec(obj, seen: set):
    """Recursively finds size of objects"""

    obj_id = id(obj)
    if obj_id in seen:
        return 0

    seen.add(obj_id)
    size = getsizeof(obj)
    if isinstance(obj, dict):
        size += sum(get_size_rec(v, seen) for v in obj.values())
        size += sum(get_size_rec(k, seen) for k in obj.keys())
    elif hasattr(obj, '__dict__'):
        size += get_size_rec(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum(get_size_rec(i, seen) for i in obj)
    return size

class BaseStream(Stream):
    def __init__(self, file_path: str):
        super().__init__()
        self._file_path = file_path
        self._file = open(file_path)
        self._lines_read = 0
        self._stats = []

    def __next__(self):
        if self._file.closed or self._should_stop():
            raise StopIteration()
        try:
            return next(self._file)
        except StopIteration:
            self._file.close()
            raise
        finally:
            self._lines_read += 1
            self._on_line_read()

    def restart(self):
        if not self._file.closed:
            self._file.close() 
        self._file = open(self._file_path)
        self._lines_read = 0
        self._on_restart()

    def finalize(self):
        self._on_finalize()

    def _should_stop(self):
        return False
    
    def _on_line_read(self):
        pass

    def _on_finalize(self):
        pass

    def _on_restart(self):
        pass

    def get_stats(self):
        return self._stats

class TimeoutStream(BaseStream):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._process = psutil.Process(os.getpid())
        self._start_time = time.perf_counter_ns()
        self._run = []
    
    def _on_line_read(self):
        if (self._lines_read - 1) % 10000:
            return
        self._run.append(self.get_current_mem_usage())

    def _should_stop(self):
        return (time.perf_counter_ns() - self._start_time) > 30_000_000_000
    
    def _on_finalize(self):
        avg_mem = int(sum(self._run) / len(self._run))
        self._stats.append((self._lines_read - 1, avg_mem))
    
    def _on_restart(self):
        self._start_time = time.perf_counter_ns()
        self._run = []

    def get_current_mem_usage(self):
        return self._process.memory_info().rss

class MemoryReadingStream(BaseStream):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._run = []
        self._start_time = time.perf_counter_ns()
        self._timeout = False
        
    def _on_line_read(self):
        if self._lines_read % 10000:
            return
        self._run.append(self.get_current_mem_usage())
        self._timeout = (time.perf_counter_ns() - self._start_time) > 30_000_000_000
    
    def _should_stop(self):
        return self._timeout

    def _on_finalize(self):
        self._stats.append(int(sum(self._run) / len(self._run)))
    
    def _on_restart(self):
        self._start_time = time.perf_counter_ns()
        self._run = []

    @staticmethod
    def get_current_mem_usage():
        total = 0
        seen = set()
        for obj in gc.get_objects():
            if isinstance(obj, CEP):
                total += get_size_rec(obj, seen)
        return total

class CounterStream(OutputStream):
    def __init__(self):
        super().__init__()
        self.reset()

    def add_item(self, item: object):
        self.count += 1

    def get_count(self):
        return self.count

    def reset(self):
        self.count = 0


class CEPEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: dict):
        return event_payload["event_type"]


class StockEventDataFormatter(DataFormatter):
    def __init__(self):
        super().__init__(CEPEventTypeClassifier())

    def parse_event(self, raw_data: str):
        if raw_data.startswith("B"):
            event_type = "BUY"
            attrs = raw_data[4:-2].split(",")
        else:
            event_type = "SELL"
            attrs = raw_data[5:-2].split(",")

        return dict(
            event_type=event_type,
            name=attrs[1].split("=")[1],
            id=int(attrs[0].split("=")[1]),
            volume=int(attrs[2].split("=")[1]),
            price=float(attrs[3].split("=")[1]),
            stock_time=int(attrs[4].split("=")[1]),
        )

    def get_event_timestamp(self, event_payload: dict):
        return datetime.fromtimestamp(event_payload["stock_time"])


class SmartHomeEventDataFormatter(DataFormatter):
    def __init__(self):
        super().__init__(CEPEventTypeClassifier())

    def parse_event(self, raw_data: str):
        attrs = raw_data[5:-2].split(",")

        return dict(
            event_type="LOAD",
            id=int(attrs[0].split("=")[1]),
            plug_timestamp=int(attrs[1].split("=")[1]),
            value=float(attrs[2].split("=")[1]),
            plug_id=int(attrs[3].split("=")[1]),
            household_id=int(attrs[4].split("=")[1]),
        )

    def get_event_timestamp(self, event_payload: dict):
        return datetime.fromtimestamp(event_payload["plug_timestamp"])


class TaxiEventDataFormatter(DataFormatter):
    def __init__(self):
        super().__init__(CEPEventTypeClassifier())

    def parse_event(self, raw_data: str):
        attrs = raw_data[5:-2].split(",")

        return dict(
            event_type="TRIP",
            id=int(attrs[0].split("=")[1]),
            dropoff_datetime=int(attrs[4].split("=")[1]),
            pickup_zone=attrs[7].split("=")[1],
            dropoff_zone=attrs[8].split("=")[1],
        )

    def get_event_timestamp(self, event_payload: dict):
        return datetime.fromtimestamp(event_payload["dropoff_datetime"])


def create_seq_query(
        seq: list[EventPattern],
        window: int,
        *,
        consumption_policy: ConsumptionPolicy = None,
        additional_conditions: list[SimpleCondition] = []):

    return [Pattern(
        SeqOperator(*(PrimitiveEventStructure(e.event_type, e.var_name)
                    for e in seq)),
        AndCondition(
            *it.chain(additional_conditions, *(e.get_conditions() for e in seq))
        ),
        timedelta(seconds=window),
        consumption_policy
    )]


def name_is(name: str):
    return Condition("name", lambda x: x == name)


BASE_STOCK_QUERY = [
    EventPattern("SELL", "T1", [name_is("INTC")]),
    EventPattern("BUY", "T2", [name_is("RIMM")]),
    EventPattern("BUY", "T3", [name_is("QQQ")]),
    EventPattern("SELL", "T4", [name_is("IPIX")]),
    EventPattern("BUY", "T5", [name_is("AMAT")]),
    EventPattern("BUY", "T6", [name_is("CSCO")]),
    EventPattern("SELL", "T7", [name_is("YHOO")]),
    EventPattern("BUY", "T8", [name_is("DELL")]),
    EventPattern("BUY", "T9", [name_is("ORCL")]),
    EventPattern("SELL", "T10", [name_is("MSFT")]),
    EventPattern("BUY", "T11", [name_is("INTC")]),
    EventPattern("BUY", "T12", [name_is("RIMM")]),
    EventPattern("SELL", "T13", [name_is("INTC")]),
    EventPattern("BUY", "T14", [name_is("RIMM")]),
    EventPattern("BUY", "T15", [name_is("QQQ")]),
    EventPattern("SELL", "T16", [name_is("IPIX")]),
    EventPattern("BUY", "T17", [name_is("AMAT")]),
    EventPattern("BUY", "T18", [name_is("CSCO")]),
    EventPattern("SELL", "T19", [name_is("YHOO")]),
    EventPattern("BUY", "T20", [name_is("DELL")]),
    EventPattern("BUY", "T21", [name_is("ORCL")]),
    EventPattern("SELL", "T22", [name_is("MSFT")]),
    EventPattern("BUY", "T23", [name_is("INTC")]),
    EventPattern("BUY", "T24", [name_is("RIMM")]),
    EventPattern("BUY", "NE", [name_is("NE")]),
]

def q1(selection_strategy: SelectionStrategies):
    return f"Q1(ss={selection_strategy.name})", create_seq_query([
        EventPattern("SELL", "msft", [name_is("MSFT")]),
        EventPattern("BUY", "oracle", [name_is("ORCL")]),
        EventPattern("BUY", "csco", [name_is("CSCO")]),
        EventPattern("SELL", "amat", [name_is("AMAT")]),
    ], 10_000, consumption_policy=ConsumptionPolicy(selection_strategy))

def q2(selection_strategy: SelectionStrategies):
    return f"Q2(ss={selection_strategy.name})", create_seq_query([
        EventPattern("SELL", "msft", [name_is("MSFT"), Condition("price", lambda x: x > 26)]),
        EventPattern("BUY", "oracle", [name_is("ORCL"), Condition("price", lambda x: x > 11.14)]),
        EventPattern("BUY", "csco", [name_is("CSCO")]),
        EventPattern("SELL", "amat", [name_is("AMAT"), Condition("price", lambda x: x >= 18.92)]),
    ], 10_000, consumption_policy=ConsumptionPolicy(selection_strategy))

def q3(selection_strategy: SelectionStrategies):
    volume_getter = lambda x: x["volume"]
    return f"Q3(ss={selection_strategy.name})", create_seq_query([
        EventPattern("SELL", "msft", [name_is("MSFT")]),
        EventPattern("BUY", "oracle", [name_is("ORCL")]),
        EventPattern("BUY", "csco", [name_is("CSCO")]),
        EventPattern("SELL", "amat", [name_is("AMAT")]),
    ], 
    10_000, 
    consumption_policy=ConsumptionPolicy(selection_strategy), 
    additional_conditions=[SimpleCondition(
        Variable("msft", volume_getter), 
        Variable("oracle", volume_getter), 
        Variable("csco", volume_getter), 
        Variable("amat", volume_getter),
        relation_op= lambda a,b,c,d: a == b == c == d)])


def household_id_is(id: int):
    return Condition("household_id", relation_op=lambda x: x == id)


VALUE_OVER_76_CONDITION = Condition("value", lambda x: x > 76)


BASE_HOMES_QUERY = [
    EventPattern("LOAD", "L1", [household_id_is(0), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L2", [household_id_is(2), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L3", [household_id_is(4), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L4", [household_id_is(6), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L5", [household_id_is(9), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L6", [household_id_is(10), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L7", [household_id_is(12), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L8", [household_id_is(14), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L9", [household_id_is(15), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L10", [household_id_is(4), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L11", [household_id_is(9), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L12", [household_id_is(10), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L13", [household_id_is(0), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L14", [household_id_is(2), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L15", [household_id_is(4), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L16", [household_id_is(6), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L17", [household_id_is(9), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L18", [household_id_is(10), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L19", [household_id_is(12), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L20", [household_id_is(14), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L21", [household_id_is(15), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L22", [household_id_is(4), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L23", [household_id_is(9), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "L24", [household_id_is(10), VALUE_OVER_76_CONDITION]),
    EventPattern("LOAD", "NE", [household_id_is(1000), VALUE_OVER_76_CONDITION])
]

def pickup_zone_is(pickup_zone: str):
    return Condition("pickup_zone", lambda x: x == pickup_zone)

def dropoff_zone_is(dropoff_zone: str):
    return Condition("dropoff_zone", lambda x: x == dropoff_zone)

BASE_TAXI_QUERY = [
    EventPattern("TRIP", "T1", [pickup_zone_is("East Harlem North"), dropoff_zone_is("Midwood")]),
    EventPattern("TRIP", "T2", [pickup_zone_is("Midwood"), dropoff_zone_is("Gravesend")]),
    EventPattern("TRIP", "T3", [pickup_zone_is("Gravesend"), dropoff_zone_is("West Brighton")]),
    EventPattern("TRIP", "T4", [pickup_zone_is("West Brighton"), dropoff_zone_is("Lincoln Square West")]),
    EventPattern("TRIP", "T5", [pickup_zone_is("Lincoln Square West"), dropoff_zone_is("Sutton Place/Turtle Bay North")]),
    EventPattern("TRIP", "T6", [pickup_zone_is("Sutton Place/Turtle Bay North"), dropoff_zone_is("East Concourse/Concourse Village")]),
    EventPattern("TRIP", "T7", [pickup_zone_is("East Concourse/Concourse Village"), dropoff_zone_is("East Harlem North")]),
    EventPattern("TRIP", "T8", [pickup_zone_is("East Harlem North"), dropoff_zone_is("East Harlem North")]),
    EventPattern("TRIP", "T9", [pickup_zone_is("East Harlem North"), dropoff_zone_is("Gravesend")]),
    EventPattern("TRIP", "T10", [pickup_zone_is("Gravesend"), dropoff_zone_is("Midwood")]),
    EventPattern("TRIP", "T11", [pickup_zone_is("Midwood"), dropoff_zone_is("Midwood")]),
    EventPattern("TRIP", "T12", [pickup_zone_is("Midwood"), dropoff_zone_is("Gravesend")]),
    EventPattern("TRIP", "T13", [pickup_zone_is("Gravesend"), dropoff_zone_is("Midwood")]),
    EventPattern("TRIP", "T14", [pickup_zone_is("Midwood"), dropoff_zone_is("Gravesend")]),
    EventPattern("TRIP", "T15", [pickup_zone_is("Gravesend"), dropoff_zone_is("West Brighton")]),
    EventPattern("TRIP", "T16", [pickup_zone_is("West Brighton"), dropoff_zone_is("Lincoln Square West")]),
    EventPattern("TRIP", "T17", [pickup_zone_is("Lincoln Square West"), dropoff_zone_is("Sutton Place/Turtle Bay North")]),
    EventPattern("TRIP", "T18", [pickup_zone_is("Sutton Place/Turtle Bay North"), dropoff_zone_is("East Concourse/Concourse Village")]),
    EventPattern("TRIP", "T19", [pickup_zone_is("East Concourse/Concourse Village"), dropoff_zone_is("East Harlem North")]),
    EventPattern("TRIP", "T20", [pickup_zone_is("East Harlem North"), dropoff_zone_is("East Harlem North")]),
    EventPattern("TRIP", "T21", [pickup_zone_is("East Harlem North"), dropoff_zone_is("Gravesend")]),
    EventPattern("TRIP", "T22", [pickup_zone_is("Gravesend"), dropoff_zone_is("Midwood")]),
    EventPattern("TRIP", "T23", [pickup_zone_is("Midwood"), dropoff_zone_is("Midwood")]),
    EventPattern("TRIP", "T24", [pickup_zone_is("Midwood"), dropoff_zone_is("Gravesend")]),
    EventPattern("TRIP", "NE"),
]

@dataclass
class SystemBenchmark:
    stream_filepath: str
    queries: list[tuple[str,Pattern]]
    formatter: DataFormatter

    @staticmethod
    def for_stocks():
        return SystemBenchmark("input_streams/stocks.stream", standard_queries(BASE_STOCK_QUERY, 1000), StockEventDataFormatter())
    
    @staticmethod
    def for_smart_homes():
        return SystemBenchmark("input_streams/smarthomes.stream", standard_queries(BASE_HOMES_QUERY, 1), SmartHomeEventDataFormatter())
    
    @staticmethod
    def for_taxis():
        return SystemBenchmark("input_streams/taxi.stream", standard_queries(BASE_TAXI_QUERY, 1000), TaxiEventDataFormatter())

    @staticmethod
    def for_old_stocks(selection_strategy: SelectionStrategies = SelectionStrategies.MATCH_ANY):
        return SystemBenchmark("input_streams/stocks.stream", [q1(selection_strategy), q2(selection_strategy), q3(selection_strategy)], StockEventDataFormatter())


def seq(base_query: list[EventPattern], length: int, window: int, selection_strategy: SelectionStrategies):
    return create_seq_query(
        base_query[:length],
        window=window,
        consumption_policy=ConsumptionPolicy(selection_strategy)
    )


def no_output(
        base_query: list[EventPattern],
        length: int,
        window: int,
        selection_strategy: SelectionStrategies):
    return create_seq_query(
        base_query[:length] + base_query[-1:],
        window=window,
        consumption_policy=ConsumptionPolicy(selection_strategy)
    )


def build_seq_pattern(
        base_query: list[EventPattern],
        length: int,
        window: int,
        selection_strategy: SelectionStrategies,
        with_fake_event: bool = False):
    if with_fake_event:
        return (
            f"no_output_seq(len={length}+1, window={window}, ss={selection_strategy.name})",
            no_output(base_query, length, window, selection_strategy)
        )
    return (
        f"seq(len={length}, window={window}, ss={selection_strategy.name})",
        seq(base_query, length, window, selection_strategy)
    )

def standard_queries(base_query: list[EventPattern], window_multiplier: int):
    seq_patterns = [
        build_seq_pattern(base_query=base_query, length=length, window=10 * window_multiplier,
                          selection_strategy=SelectionStrategies.MATCH_ANY)
        for length in (3, 6, 9, 12)
    ]
    no_output_patterns = [
        build_seq_pattern(base_query=base_query, length=length, window=10 * window_multiplier,
                          selection_strategy=SelectionStrategies.MATCH_ANY, with_fake_event=True)
        for length in (3, 6, 9, 12)
    ]
    window_patterns = [
        build_seq_pattern(base_query=base_query, length=3, window=window * window_multiplier,
                          selection_strategy=SelectionStrategies.MATCH_ANY, with_fake_event=True)
        for window in (20, 30, 40)
    ]
    window_patterns_next = [
        build_seq_pattern(base_query=base_query, length=3, window=window * window_multiplier,
                          selection_strategy=SelectionStrategies.MATCH_NEXT, with_fake_event=True)
        for window in (10, 20, 30, 40)
    ]
    # selection_strategy_patterns = [
    #     build_seq_pattern(base_query=benchmark.base_query, length=3, window=10 * benchmark.window_multiplier,
    #                       selection_strategy=SelectionStrategies.MATCH_SINGLE),
    #     build_seq_pattern(base_query=benchmark.base_query, length=3, window=10 * benchmark.window_multiplier,
    #                       selection_strategy=SelectionStrategies.MATCH_ANY),
    #     build_seq_pattern(base_query=benchmark.base_query, length=3, window=10 * benchmark.window_multiplier,
    #                       selection_strategy=SelectionStrategies.MATCH_SINGLE,
    #                       with_fake_event=True),
    #     build_seq_pattern(base_query=benchmark.base_query, length=3, window=10 * benchmark.window_multiplier,
    #                       selection_strategy=SelectionStrategies.MATCH_ANY,
    #                       with_fake_event=True),
    # ]

    return list(it.chain(seq_patterns, no_output_patterns, window_patterns, window_patterns_next))

def run_benchmark(
        benchmark: SystemBenchmark,
        repeat=2,
        for_memory=False,
        out_file_name: str = None):

    results = []

    for title, pattern in benchmark.queries:
        stream = MemoryReadingStream(benchmark.stream_filepath) if for_memory else TimeoutStream(benchmark.stream_filepath)
        times = timeit.repeat(
                "cep.run(stream, counter, formater); stream.finalize()",
                "stream.restart(); cep = CEP(pattern); counter.reset(); gc.collect()",
                globals=dict(
                    CEP=CEP,
                    counter=CounterStream(),
                    stream=stream,
                    pattern=pattern,
                    gc=gc,
                    formater=benchmark.formatter),
                repeat=repeat,
                number=1)
        if for_memory:
            headers = ("Run", "Time (s)", "Avg. memory (bytes)")
            table = [(run, runtime, mem) for run, (runtime, mem) in enumerate(zip(times, stream.get_stats()), start=1)]
        else:
            headers = ("Run", "Time (s)", "Events", "Avg. memory (bytes)")
            table = [(run, runtime, evts, mem) for run, (runtime, (evts, mem)) in enumerate(zip(times, stream.get_stats()), start=1)]

        print("\n * " + title)
        print(tabulate(table, headers, tablefmt="orgtbl"), "\n")

        if for_memory or out_file_name is None: continue
        query_results = []
        for _, rt, ev, mem in table:
            query_results.extend((rt, ev, mem))
        results.append(query_results)

    if for_memory or out_file_name is None:
        return

    with open(out_file_name, "w") as f:
        f.writelines(",".join(str(r) for r in line) + "\n" for line in results)


if __name__ == "__main__":
    # print("\nSTOCKS (OLD QUERIES)\n")
    # run_benchmark(SystemBenchmark.for_old_stocks(), repeat=2, out_file_name="outputs/stocks_old.csv")
    # print("\nSTOCKS\n")
    # run_benchmark(SystemBenchmark.for_stocks(), repeat=2, out_file_name="outputs/stocks.csv")
    print("\nSMART HOMES\n")
    run_benchmark(SystemBenchmark.for_smart_homes(), repeat=2, out_file_name="outputs/smart_homes.csv")
    # print("\nTAXI\n")
    # run_benchmark(SystemBenchmark.for_taxis(), repeat=2, out_file_name="outputs/taxi.csv")