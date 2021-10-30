import time
import os
import string
import numpy.random as rd
import subprocess
import sys

# TESTS = ['Time', 'Memory']
TESTS = ['Time']
STREAM_EVENTS = [3, 5, 7]
STREAM_LENGTH = 60_000_000
WIN_LENGTH = [100, 200, 300]
NOISE_EVENTS = [6]
ITERATIONS = 3
TIMEOUTS = [30]
SYSTEMS = ['flink', 'esper8', 'core', 'sase', 'core1']
# SYSTEMS = ['esper8']
# SYSTEMS = ['flink']
# SYSTEMS = ['core']
TEST_NAME = 'kleene2'
CONSUME = True
NUM_EVENT_DICT = {}


def create_folder():
    os.mkdir(f'./results/{TEST_NAME}')

def create_streams():
    print('Creating streams...')
    os.mkdir(f'./results/{TEST_NAME}/streams')
    for events_num in STREAM_EVENTS:
        for noise_events in NOISE_EVENTS:
            events = get_events(events_num + noise_events)
            curr_events = rd.choice(events, size=STREAM_LENGTH)
            with open(f'./results/{TEST_NAME}/streams/{events_num}_{noise_events}_{STREAM_LENGTH}.stream', 'w') as tf:
                for i in range(STREAM_LENGTH):
                    tf.write(f'{curr_events[i]}(id={i})\n')
    print('Finished writing streams.')


def get_events(events_num):
    events = []
    count = 0
    for a in string.ascii_uppercase:
        for b in string.ascii_uppercase:
            if a + b == 'AS' or a + b == 'AT':
                continue
            events.append(a + b)
            count += 1
            if count == events_num:
                return events


def create_queries():
    print('Creating queries...')
    for system in SYSTEMS:
        if system == 'core':
            create_core_query()
        elif system == 'sase':
            create_sase_query()
        elif system == 'esper8':
            create_esper_query()
    print('Finished creating queries.')


def create_core_query():
    os.mkdir(f'./results/{TEST_NAME}/core')
    for events_num in STREAM_EVENTS:
        noise_events = max(NOISE_EVENTS)
        events = get_events(events_num + noise_events)
        events_query = [events[i] for i in range(events_num)]
        for win_length in WIN_LENGTH:
            with open(f'./results/{TEST_NAME}/core/core_declaration_{events_num}_{win_length}.query', 'w') as tf:
                tf.write(
                    f'FILE:./results/{TEST_NAME}/core/core_metadata_{events_num}_{win_length}.meta\n')
                tf.write(
                    f'FILE:./results/{TEST_NAME}/core/core_{events_num}_{win_length}.query\n')
            with open(f'./results/{TEST_NAME}/core/core_metadata_{events_num}_{win_length}.meta', 'w') as tf:
                for event in events:
                    tf.write(f'DECLARE EVENT {event}(id long)\n')
                tf.write(f'DECLARE STREAM S({", ".join(events)})')
            with open(f'./results/{TEST_NAME}/core/core_{events_num}_{win_length}.query', 'w') as tf:
                tf.write('SELECT ALL *\n')
                tf.write('FROM S\n')
                tf.write('WHERE (')
                for i in range(len(events_query)):
                    if i == 0:
                        tf.write(f'{events_query[i]}')
                    elif i % 2:
                        tf.write(f'; {events_query[i]}+')
                    else:
                        tf.write(f'; {events_query[i]}')
                tf.write(')\n')
                tf.write(f'WITHIN {win_length} EVENTS\n')
                if CONSUME:
                    tf.write('CONSUME BY ANY')
                else:
                    tf.write('CONSUME BY NONE')
        for noise_events in NOISE_EVENTS:
            with open(f'./results/{TEST_NAME}/core/{events_num}_{noise_events}_{STREAM_LENGTH}.stream', 'w') as tf:
                tf.write(
                    f'S:FILE:./results/{TEST_NAME}/streams/{events_num}_{noise_events}_{STREAM_LENGTH}.stream')


def create_sase_query():
    os.mkdir(f'./results/{TEST_NAME}/sase')
    for events_num in STREAM_EVENTS:
        noise_events = max(NOISE_EVENTS)
        events = get_events(events_num + noise_events)
        events_query = [events[i] for i in range(events_num)]
        for win_length in WIN_LENGTH:
            with open(f'./results/{TEST_NAME}/sase/sase_{events_num}_{win_length}.query', 'w') as tf:
                tf.write('PATTERN SEQ(')
                for i in range(len(events_query)):
                    if i == 0:
                        tf.write(f'{events_query[i] + " " + events_query[i].lower()}')
                    elif i % 2:
                        tf.write(f', {events_query[i] + "+ " + events_query[i].lower()}')
                    else:
                        tf.write(f', {events_query[i] + " " + events_query[i].lower()}')
                tf.write(')\n')
                tf.write('WHERE skip-till-any-match\n')
                tf.write(f'WITHIN {win_length - 1}')


def create_esper_query():
    os.mkdir(f'./results/{TEST_NAME}/esper')
    for events_num in STREAM_EVENTS:
        noise_events = max(NOISE_EVENTS)
        events = get_events(events_num + noise_events)
        events_query = [events[i] for i in range(events_num)]
        events_lower = [a.lower() for a in events_query]
        for win_length in WIN_LENGTH:
            with open(f'./results/{TEST_NAME}/esper/esper_{events_num}_{win_length}.query', 'w') as tf:
                tf.write(
                    f'select {", ".join(events_lower)} from MyEvent#length({win_length})\n')
                tf.write('match_recognize (\n')
                tf.write('measures ')
                for i in range(events_num):
                    if i == 0:
                        pass
                    else:
                        tf.write(', ')
                    tf.write(events[i] + " as " + events_lower[i])
                tf.write('\n')
                tf.write('all matches\n')
                tf.write(f'pattern (')
                for i in range(len(events_query)):
                    if i == 0:
                        tf.write(f'{events_query[i]} s*')
                    elif i == len(events_query) - 1:
                        if i % 2:
                            tf.write(f' {events_query[i]} ({events_query[i]} | s)*')
                        else:
                            tf.write(f' {events_query[i]}')
                    elif i % 2:
                        tf.write(f' {events_query[i]} ({events_query[i]} | s)*')
                    else:
                        tf.write(f' {events_query[i]} s*')
                tf.write(')\n')
                tf.write('define\n')
                last = events_query[-1]
                for event in events_query:
                    if event == last:
                        tf.write(f'    {event} as {event}.type = "{event}")')
                    else:
                        tf.write(f'    {event} as {event}.type = "{event}",\n')


def run_systems():
    print('Running systems...')
    if not os.path.exists(f'./results/{TEST_NAME}/results'):
        os.mkdir(f'./results/{TEST_NAME}/results')
    for events_num in STREAM_EVENTS:
        print(f'Running queries with {events_num} events...')
        for test in TESTS:
            print(f'Running {test} test...')
            for system in SYSTEMS:
                print(f'Running {system}...')
                memorytest = False
                if test == 'Memory':
                    memorytest = True
                for win_length in WIN_LENGTH:
                    for noise_events in NOISE_EVENTS:
                        for j in range(len(TIMEOUTS)):
                            timeout = TIMEOUTS[j]
                            for i in range(ITERATIONS):
                                data = (system, events_num, win_length, noise_events, timeout)
                                max_events = STREAM_LENGTH
                                if memorytest:
                                    if data not in NUM_EVENT_DICT:
                                        break
                                    max_events = int(sum(NUM_EVENT_DICT[data])/ITERATIONS)
                                try:
                                    if system == 'core':
                                        print(
                                            f'Running core with query core_{events_num}_{win_length}.query, stream {events_num}_{noise_events}_{STREAM_LENGTH}.stream. Memorytest: {memorytest}')
                                        t0 = time.time_ns()
                                        res = run_core(
                                            events_num, win_length, noise_events, memorytest, timeout, max_events)
                                        total_time = time.time_ns() - t0
                                    elif system == 'core1':
                                        print(
                                            f'Running core1 with query core_{events_num}_{win_length}.query, stream {events_num}_{noise_events}_{STREAM_LENGTH}.stream. Memorytest: {memorytest}')
                                        t0 = time.time_ns()
                                        res = run_core1(
                                            events_num, win_length, noise_events, memorytest, timeout, max_events)
                                        total_time = time.time_ns() - t0
                                    elif system == 'sase':
                                        print(
                                            f'Running sase with query sase_{events_num}_{win_length}.query, stream {events_num}_{noise_events}_{STREAM_LENGTH}.stream. Memorytest: {memorytest}')
                                        t0 = time.time_ns()
                                        res = run_sase(
                                            events_num, win_length, noise_events, memorytest, timeout, max_events)
                                        total_time = time.time_ns() - t0
                                    elif system == 'esper':
                                        print(
                                            f'Running esper with query esper_{events_num}_{win_length}.query, stream {events_num}_{noise_events}_{STREAM_LENGTH}.stream. Memorytest: {memorytest}')
                                        if not CONSUME:
                                            break
                                        t0 = time.time_ns()
                                        res = run_esper(
                                            events_num, win_length, noise_events, memorytest, timeout, max_events)
                                        total_time = time.time_ns() - t0
                                    elif system == 'esper8':
                                        print(
                                            f'Running esper8 with query esper_{events_num}_{win_length}.query, stream {events_num}_{noise_events}_{STREAM_LENGTH}.stream. Memorytest: {memorytest}')
                                        if not CONSUME:
                                            break
                                        t0 = time.time_ns()
                                        res = run_esper8(
                                            events_num, win_length, noise_events, memorytest, timeout, max_events)
                                        total_time = time.time_ns() - t0
                                    elif system == 'flink':
                                        print(
                                            f'Running flink with query flink_{events_num}_{win_length}.query, stream {events_num}_{noise_events}_{STREAM_LENGTH}.stream. Memorytest: {memorytest}')
                                        if not CONSUME:
                                            break
                                        t0 = time.time_ns()
                                        res = run_flink(
                                            events_num, win_length, noise_events, memorytest, timeout, max_events)
                                        total_time = time.time_ns() - t0
                                    else:
                                        sys.exit(1)
                                    with open(f'./results/{TEST_NAME}/results/{system}_{events_num}_{win_length}_{noise_events}_{test}.query' + '_out.txt', 'ab') as tf:
                                        if not j and not i:
                                            if memorytest:
                                                tf.write(
                                                    b'MAXTotal,AVGTotal,MAXUsed,AVGUsed\n')
                                            else:
                                                tf.write(
                                                    b'Timeout,NumberOfEvents,EnumTime,Matches\n')
                                        if not memorytest:
                                            tf.write(f'{timeout},'.encode())
                                        tf.write(res.stdout)
                                    with open(f'./results/{TEST_NAME}/results/{system}_{events_num}_{win_length}_{noise_events}_{test}.query' + '_err.txt', 'ab') as tf:
                                        tf.write(res.stderr)
                                    print(
                                        f'successfully ran {system} query {system}_{events_num}_{win_length}.query with stream {events_num}_{noise_events}_{STREAM_LENGTH}.stream.')
                                    if not memorytest:
                                        events = res.stdout.decode().split(',')[0]
                                        if data in NUM_EVENT_DICT:
                                            NUM_EVENT_DICT[data].append(int(events))
                                        else:
                                            NUM_EVENT_DICT[data] = [int(events)]
                                except subprocess.TimeoutExpired as err:
                                    with open(f'./results/{TEST_NAME}/results/{system}_{events_num}_{win_length}_{noise_events}_{test}.query' + '_except.txt', 'a') as tf:
                                        tf.write('query timeout:\n')
                                        tf.write(str(err.timeout))
                                        tf.write('\n')
                                        tf.write(str(err.cmd))
                                        tf.write('\n')
                                        tf.write(err.output.decode())
                                        tf.write('\n')
                                        tf.write(err.stderr.decode())
                                        break
                                except subprocess.CalledProcessError as err:
                                    with open(f'./results/{TEST_NAME}/results/{system}_{events_num}_{win_length}_{noise_events}_{test}.query' + '_except.txt', 'a') as tf:
                                        tf.write('query error:\n')
                                        tf.write(str(err.returncode))
                                        tf.write('\n')
                                        tf.write(str(err.cmd))
                                        tf.write('\n')
                                        tf.write(err.output.decode())
                                        tf.write('\n')
                                        tf.write(err.stderr.decode())
                                        break
                                except Exception as err:
                                    with open(f'./results/{TEST_NAME}/results/{system}_{events_num}_{win_length}_{noise_events}_{test}.query' + '_except.txt', 'a') as tf:
                                        tf.write('query error:\n')
                                        tf.write(str(err))
                                        break
                print(f'Finished running {system}.')
        print(f'Finished running {test} test...')
    print('Finished Running systems.')


def run_core(events_num, win_length, noise_events, memorytest, timeout, max_events):
    return subprocess.run(['java', '-Xmx50G',
                           '-jar', './jars/core.main.jar',
                           '-of',
                           '-q', f'./results/{TEST_NAME}/core/core_declaration_{events_num}_{win_length}.query',
                           '-s', f'./results/{TEST_NAME}/core/{events_num}_{noise_events}_{STREAM_LENGTH}.stream',
                           '-m', f'{memorytest}',
                           '-t', f'{timeout}',
                           '-n', f'{max_events}'],
                          timeout=timeout * 10, capture_output=True, check=True)

def run_core1(events_num, win_length, noise_events, memorytest, timeout, max_events):
    return subprocess.run(['java', '-Xmx50G',
                           '-jar', './jars/core.main.jar',
                           '-of',
                           '-q', f'./results/{TEST_NAME}/core/core_declaration_{events_num}_{win_length}.query',
                           '-s', f'./results/{TEST_NAME}/core/{events_num}_{noise_events}_{STREAM_LENGTH}.stream',
                           '-m', f'{memorytest}',
                           '-t', f'{timeout}',
                           '-n', f'{max_events}',
                           '-i', '1'],
                          timeout=timeout * 10, capture_output=True, check=True)


def run_sase(events_num, win_length, noise_events, memorytest, timeout, max_events):
    return subprocess.run(['java', '-Xmx50G',
                           '-jar', './jars/sase.jar',
                           f'./results/{TEST_NAME}/sase/sase_{events_num}_{win_length}.query',
                           f'./results/{TEST_NAME}/streams/{events_num}_{noise_events}_{STREAM_LENGTH}.stream',
                           f'{CONSUME}', f'{memorytest}', f'{True}', f'{max_events}', f'{False}', f'{timeout}'],
                          timeout=timeout * 10, capture_output=True, check=True)


def run_esper(events_num, win_length, noise_events, memorytest, timeout, max_events):
    return subprocess.run(['java', '-Xmx50G',
                           '-jar', './jars/esper.jar',
                           f'./results/{TEST_NAME}/esper/esper_{events_num}_{win_length}.query',
                           f'./results/{TEST_NAME}/streams/{events_num}_{noise_events}_{STREAM_LENGTH}.stream',
                           f'{memorytest}', f'{True}', f'{max_events}', f'{timeout}'],
                          timeout=timeout * 10, capture_output=True, check=True)

def run_esper8(events_num, win_length, noise_events, memorytest, timeout, max_events):
    return subprocess.run(['java', '-Xmx50G',
                           '-jar', './jars/esper8.jar',
                           f'./results/{TEST_NAME}/esper/esper_{events_num}_{win_length}.query',
                           f'./results/{TEST_NAME}/streams/{events_num}_{noise_events}_{STREAM_LENGTH}.stream',
                           f'{memorytest}', f'{True}', f'{max_events}', f'{timeout}'],
                          timeout=timeout * 10, capture_output=True, check=True)

def run_flink(events_num, win_length, noise_events, memorytest, timeout, max_events):
    return subprocess.run(["java", "-Dfile.encoding=windows-1252", "-Duser.country=US", "-Duser.language=en", "-Duser.variant", 
                            "--add-opens", "java.base/java.lang=ALL-UNNAMED",
                            "-cp", "./jars/flink/lib/flink-1.0-SNAPSHOT.jar;./jars/flink/lib/flink-clients_2.12-1.12.2.jar;./jars/flink/lib/flink-streaming-java_2.12-1.12.2.jar;./jars/flink/lib/flink-cep_2.12-1.12.2.jar;./jars/flink/lib/flink-file-sink-common-1.12.2.jar;./jars/flink/lib/flink-optimizer_2.12-1.12.2.jar;./jars/flink/lib/flink-runtime_2.12-1.12.2.jar;./jars/flink/lib/flink-java-1.12.2.jar;./jars/flink/lib/flink-hadoop-fs-1.12.2.jar;./jars/flink/lib/flink-core-1.12.2.jar;./jars/flink/lib/flink-queryable-state-client-java-1.12.2.jar;./jars/flink/lib/flink-shaded-guava-18.0-12.0.jar;./jars/flink/lib/commons-math3-3.5.jar;./jars/flink/lib/flink-annotations-1.12.2.jar;./jars/flink/lib/akka-slf4j_2.12-2.5.21.jar;./jars/flink/lib/grizzled-slf4j_2.12-1.3.2.jar;./jars/flink/lib/slf4j-api-1.7.25.jar;./jars/flink/lib/jsr305-1.3.9.jar;./jars/flink/lib/flink-metrics-core-1.12.2.jar;./jars/flink/lib/force-shading-1.12.2.jar;./jars/flink/lib/commons-cli-1.3.1.jar;./jars/flink/lib/flink-shaded-asm-7-7.1-12.0.jar;./jars/flink/lib/commons-lang3-3.3.2.jar;./jars/flink/lib/kryo-2.24.0.jar;./jars/flink/lib/commons-collections-3.2.2.jar;./jars/flink/lib/commons-compress-1.20.jar;./jars/flink/lib/commons-io-2.7.jar;./jars/flink/lib/flink-shaded-netty-4.1.49.Final-12.0.jar;./jars/flink/lib/flink-shaded-jackson-2.10.1-12.0.jar;./jars/flink/lib/flink-shaded-zookeeper-3-3.4.14-12.0.jar;./jars/flink/lib/javassist-3.24.0-GA.jar;./jars/flink/lib/scala-library-2.12.7.jar;./jars/flink/lib/akka-stream_2.12-2.5.21.jar;./jars/flink/lib/akka-actor_2.12-2.5.21.jar;./jars/flink/lib/akka-protobuf_2.12-2.5.21.jar;./jars/flink/lib/scopt_2.12-3.5.0.jar;./jars/flink/lib/snappy-java-1.1.4.jar;./jars/flink/lib/chill_2.12-0.7.6.jar;./jars/flink/lib/lz4-java-1.6.0.jar;./jars/flink/lib/minlog-1.2.jar;./jars/flink/lib/objenesis-2.1.jar;./jars/flink/lib/config-1.3.3.jar;./jars/flink/lib/scala-java8-compat_2.12-0.8.0.jar;./jars/flink/lib/reactive-streams-1.0.2.jar;./jars/flink/lib/ssl-config-core_2.12-0.3.7.jar;./jars/flink/lib/chill-java-0.7.6.jar;./jars/flink/lib/scala-parser-combinators_2.12-1.1.1.jar",
                            "edu.puc.flink.Main",
                            f'./results/{TEST_NAME}/streams/{events_num}_{noise_events}_{STREAM_LENGTH}.stream',
                            "seq", f"{events_num} {win_length}", 'false', f'{timeout}', f'{memorytest}', f'{max_events}'],
                          timeout=timeout * 10, capture_output=True, check=True)


def main():
    print(f'Starting test with TEST_NAME {TEST_NAME}')
    create_folder()
    create_streams()
    create_queries()
    run_systems()


if __name__ == "__main__":
    main()
