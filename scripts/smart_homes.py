import time
import os
import string
import numpy.random as rd
import subprocess
import sys

TESTS = ['Time', "Memory"]
# TESTS = ["Memory"]
WIN_LENGTH = [10, 20, 30, 40]
# WIN_LENGTH = [10000]
# WIN_LENGTH = [100]
ITERATIONS = 3
TIMEOUTS = [30]
SYSTEMS = ['esper8', 'flink', 'sase',  'core']
# SYSTEMS = ['esper8', 'core', 'sase']
# SYSTEMS = ['flink']
TEST_NAME = ['smart_home']
CONSUME = True
NUM_EVENT_DICT = {}

def create_folder():
    os.mkdir(f'./results/{TEST_NAME[0]}')


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
    os.mkdir(f'./results/{TEST_NAME[0]}/core')
    with open(f'./results/{TEST_NAME[0]}/core/smarthomes.stream', 'w') as tf:
        tf.write('S:FILE:./smarthomestream/smarthomes.stream')
    for i in range(1, 13):
        with open(f'./smarthomequeries/CORE/q{i}.txt') as tf:
            original = tf.read()
        for win_length in WIN_LENGTH:
            with open(f'./results/{TEST_NAME[0]}/core/core_declaration_smarthomes_q{i}_{win_length}.query', 'w') as tf:
                tf.write('FILE:./smarthomequeries/CORE/descriptions/core.txt\n')
                tf.write(
                    f'FILE:./results/{TEST_NAME[0]}/core/core_smarthomes_q{i}_{win_length}.query\n')
            with open(f'./results/{TEST_NAME[0]}/core/core_smarthomes_q{i}_{win_length}.query', 'w') as tf:
                tf.write(original.replace('TIMESTAMP', f'{win_length}'))


def create_sase_query():
    os.mkdir(f'./results/{TEST_NAME[0]}/sase')
    for i in range(1, 13):
        with open(f'./smarthomequeries/SASE/q{i}.query') as tf:
            original = tf.read()
        for win_length in WIN_LENGTH:
            with open(f'./results/{TEST_NAME[0]}/sase/sase_smarthomes_q{i}_{win_length}.query', 'w') as tf:
                tf.write(original.replace('TIMESTAMP', f'{win_length - 1}'))


def create_esper_query():
    os.mkdir(f'./results/{TEST_NAME[0]}/esper')
    for i in range(1, 13):
        with open(f'./smarthomequeries/ESPER/q{i}.query') as tf:
            original = tf.read()
        for win_length in WIN_LENGTH:
            with open(f'./results/{TEST_NAME[0]}/esper/esper_smarthomes_q{i}_{win_length}.query', 'w') as tf:
                tf.write(original.replace('TIMESTAMP', f'{win_length}'))


def run_systems():
    print('Running systems...')
    if not os.path.exists(f'./results/{TEST_NAME[0]}/results'):
        os.mkdir(f'./results/{TEST_NAME[0]}/results')
    for test in TESTS:
        print(f'Running {test} test...')
        for query in range(1, 13):
            for system in SYSTEMS:
                print(f'Running {system}...')
                memorytest = False
                if test == 'Memory':
                    memorytest = True
                for win_length in WIN_LENGTH:
                    if win_length != 10 and query < 11:
                        continue
                    for j in range(len(TIMEOUTS)):
                        timeout = TIMEOUTS[j]
                        for i in range(ITERATIONS):
                            data = (system,  win_length, query, timeout)
                            max_events = 1000000
                            if memorytest:
                                if data not in NUM_EVENT_DICT:
                                    break
                                max_events = int(sum(NUM_EVENT_DICT[data])/ITERATIONS)
                            try:
                                if system == 'core':
                                    print(
                                        f'Running core with query core_smarthomes_q{query}_{win_length}.query, stream smarthomes.stream. Memorytest: {memorytest}')
                                    t0 = time.time_ns()
                                    res = run_core(win_length, query, memorytest, timeout, max_events)
                                    total_time = time.time_ns() - t0
                                elif system == 'core1':
                                    print(
                                        f'Running core1 with query core_smarthomes_q{query}_{win_length}.query, stream smarthomes.stream. Memorytest: {memorytest}')
                                    t0 = time.time_ns()
                                    res = run_core1(win_length, query, memorytest, timeout, max_events)
                                    total_time = time.time_ns() - t0
                                elif system == 'sase':
                                    print(
                                        f'Running sase with query sase_smarthomes_q{query}_{win_length}.query, stream smarthomes.stream. Memorytest: {memorytest}')
                                    t0 = time.time_ns()
                                    res = run_sase(win_length, query, memorytest, timeout, max_events)
                                    total_time = time.time_ns() - t0
                                elif system == 'esper8':
                                    print(
                                        f'Running esper8 with query esper_smarthomes_q{query}_{win_length}.query, stream smarthomes.stream. Memorytest: {memorytest}')
                                    if not CONSUME:
                                        break
                                    t0 = time.time_ns()
                                    res = run_esper8(win_length, query, memorytest, timeout, max_events)
                                    total_time = time.time_ns() - t0
                                elif system == 'flink':
                                    print(
                                        f'Running flink with query flink_smarthomes_q{query}_{win_length}.query, stream smarthomes.stream. Memorytest: {memorytest}')
                                    if not CONSUME:
                                        break
                                    t0 = time.time_ns()
                                    res = run_flink(win_length, query, memorytest, timeout, max_events)
                                    total_time = time.time_ns() - t0
                                else:
                                    continue
                                with open(f'./results/{TEST_NAME[0]}/results/{system}_smarthomes_q{query}_{win_length}_{test}.query' + '_out.txt', 'ab') as tf:
                                    if not j and not i:
                                        if memorytest:
                                            tf.write(
                                                b'MAXTotal,AVGTotal,MAXUsed,AVGUsed\n')
                                        else:
                                            tf.write(
                                                b'Timeout,TotalTime,NumberOfEvents,EnumTime,Matches\n')
                                    if not memorytest:
                                        tf.write(f'{timeout},'.encode())
                                    tf.write(res.stdout)
                                with open(f'./results/{TEST_NAME[0]}/results/{system}_smarthomes_q{query}_{win_length}_{test}.query' + '_err.txt', 'ab') as tf:
                                    tf.write(res.stderr)
                                print(
                                    f'successfully ran {system} query {system}_smarthomes_q{query}_{win_length}.query with stream smarthomes.stream.')
                                if not memorytest:
                                    events = res.stdout.decode().split(',')[1]
                                    if data in NUM_EVENT_DICT:
                                        NUM_EVENT_DICT[data].append(int(events))
                                    else:
                                        NUM_EVENT_DICT[data] = [int(events)]
                            except subprocess.TimeoutExpired as err:
                                with open(f'./results/{TEST_NAME[0]}/results/{system}_smarthomes_q{query}_{win_length}_{test}.query' + '_except.txt', 'a') as tf:
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
                                with open(f'./results/{TEST_NAME[0]}/results/{system}_smarthomes_q{query}_{win_length}_{test}.query' + '_except.txt', 'a') as tf:
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
                                with open(f'./results/{TEST_NAME[0]}/results/{system}_smarthomes_q{query}_{win_length}_{test}.query' + '_except.txt', 'a') as tf:
                                    tf.write('query error:\n')
                                    tf.write(str(err))
                                    break
            print(f'Finished running {system}.')
        print(f'Finished running {test} test...')
    print('Finished Running systems.')


def run_core(win_length, query, memorytest, timeout, max_events):
    return subprocess.run(['C:/Users/AndresQuintana/.jdks/jbr-11.0.12/bin/java', '-Xmx50G',
                           '-jar', './jars/core.main.jar',
                           '-of',
                           '-q', f'./results/{TEST_NAME[0]}/core/core_declaration_smarthomes_q{query}_{win_length}.query',
                           '-s', f'./results/{TEST_NAME[0]}/core/smarthomes.stream',
                           '-m', f'{memorytest}',
                           '-t', f'{timeout}',
                           '-n', f'{max_events}',
                           '-i', '1000',
                           '-e', 'true'],
                          timeout=timeout * 2, capture_output=True, check=True)

def run_core1(win_length, query, memorytest, timeout, max_events):
    return subprocess.run(['C:/Users/AndresQuintana/.jdks/jbr-11.0.12/bin/java', '-Xmx50G',
                           '-jar', './jars/core.main.jar',
                           '-of',
                           '-q', f'./results/{TEST_NAME[0]}/core/core_declaration_smarthomes_q{query}_{win_length}.query',
                           '-s', f'./results/{TEST_NAME[0]}/core/smarthomes.stream',
                           '-m', f'{memorytest}',
                           '-t', f'{timeout}',
                           '-n', f'{max_events}',
                           '-i', '1',
                           '-e', 'true'],
                          timeout=timeout * 2, capture_output=True, check=True)


def run_sase(win_length, query, memorytest, timeout, max_events):
    return subprocess.run(['C:/Users/AndresQuintana/.jdks/jbr-11.0.12/bin/java', '-Xmx50G',
                           '-jar', './jars/sase.jar',
                           f'./results/{TEST_NAME[0]}/sase/sase_smarthomes_q{query}_{win_length}.query',
                           f'./smarthomestream/smarthomes.stream',
                           f'{CONSUME}', f'{memorytest}', f'{True}', f'{max_events}', f'{2}', f'{timeout}'],
                          timeout=timeout * 2, capture_output=True, check=True)


def run_esper8(win_length, query, memorytest, timeout, max_events):
    return subprocess.run(['C:/Users/AndresQuintana/.jdks/jbr-11.0.12/bin/java', '-Xmx50G',
                           '-jar', './jars/esperhomes.jar',
                           f'./results/{TEST_NAME[0]}/esper/esper_smarthomes_q{query}_{win_length}.query',
                           f'./smarthomestream/smarthomes.stream',
                           f'{memorytest}', f'{True}', f'{max_events}', f'{timeout}'],
                          timeout=timeout * 2, capture_output=True, check=True)

def run_flink(win_length, query, memorytest, timeout, max_events):
    return subprocess.run(["C:/Users/AndresQuintana/.jdks/jbr-11.0.12/bin/java", "-Dfile.encoding=windows-1252", "-Duser.country=US", "-Duser.language=en", "-Duser.variant", 
                            "--add-opens", "java.base/java.lang=ALL-UNNAMED",
                            "-cp", "./jars/flink/lib/flink-1.0-SNAPSHOT.jar;./jars/flink/lib/flink-clients_2.12-1.12.2.jar;./jars/flink/lib/flink-streaming-java_2.12-1.12.2.jar;./jars/flink/lib/flink-cep_2.12-1.12.2.jar;./jars/flink/lib/flink-file-sink-common-1.12.2.jar;./jars/flink/lib/flink-optimizer_2.12-1.12.2.jar;./jars/flink/lib/flink-runtime_2.12-1.12.2.jar;./jars/flink/lib/flink-java-1.12.2.jar;./jars/flink/lib/flink-hadoop-fs-1.12.2.jar;./jars/flink/lib/flink-core-1.12.2.jar;./jars/flink/lib/flink-queryable-state-client-java-1.12.2.jar;./jars/flink/lib/flink-shaded-guava-18.0-12.0.jar;./jars/flink/lib/commons-math3-3.5.jar;./jars/flink/lib/flink-annotations-1.12.2.jar;./jars/flink/lib/akka-slf4j_2.12-2.5.21.jar;./jars/flink/lib/grizzled-slf4j_2.12-1.3.2.jar;./jars/flink/lib/slf4j-api-1.7.25.jar;./jars/flink/lib/jsr305-1.3.9.jar;./jars/flink/lib/flink-metrics-core-1.12.2.jar;./jars/flink/lib/force-shading-1.12.2.jar;./jars/flink/lib/commons-cli-1.3.1.jar;./jars/flink/lib/flink-shaded-asm-7-7.1-12.0.jar;./jars/flink/lib/commons-lang3-3.3.2.jar;./jars/flink/lib/kryo-2.24.0.jar;./jars/flink/lib/commons-collections-3.2.2.jar;./jars/flink/lib/commons-compress-1.20.jar;./jars/flink/lib/commons-io-2.7.jar;./jars/flink/lib/flink-shaded-netty-4.1.49.Final-12.0.jar;./jars/flink/lib/flink-shaded-jackson-2.10.1-12.0.jar;./jars/flink/lib/flink-shaded-zookeeper-3-3.4.14-12.0.jar;./jars/flink/lib/javassist-3.24.0-GA.jar;./jars/flink/lib/scala-library-2.12.7.jar;./jars/flink/lib/akka-stream_2.12-2.5.21.jar;./jars/flink/lib/akka-actor_2.12-2.5.21.jar;./jars/flink/lib/akka-protobuf_2.12-2.5.21.jar;./jars/flink/lib/scopt_2.12-3.5.0.jar;./jars/flink/lib/snappy-java-1.1.4.jar;./jars/flink/lib/chill_2.12-0.7.6.jar;./jars/flink/lib/lz4-java-1.6.0.jar;./jars/flink/lib/minlog-1.2.jar;./jars/flink/lib/objenesis-2.1.jar;./jars/flink/lib/config-1.3.3.jar;./jars/flink/lib/scala-java8-compat_2.12-0.8.0.jar;./jars/flink/lib/reactive-streams-1.0.2.jar;./jars/flink/lib/ssl-config-core_2.12-0.3.7.jar;./jars/flink/lib/chill-java-0.7.6.jar;./jars/flink/lib/scala-parser-combinators_2.12-1.1.1.jar",
                            "edu.puc.flink.Smart",
                            f'./smarthomestream/smarthomes.stream',
                            f"{query}", f"{win_length}", 'false', f'{timeout}', f'{memorytest}', f'{max_events}'],
                          timeout=timeout * 2, capture_output=True, check=True)

def main():
    if len(sys.argv) < 2:
        print(f'Starting test with TEST_NAME[0] {TEST_NAME[0]}')
        create_folder()
        create_queries()
    else:
        TEST_NAME[0] = int(sys.argv[1])
    run_systems()


if __name__ == "__main__":
    main()
