__author__ = 'aerospike'

import copy
import datetime
import re
from lib.logreader import COUNT_RESULT_KEY, TOTAL_ROW_HEADER, END_ROW_KEY
from lib.loglatency import LogLatency

DT_FMT = "%b %d %Y %H:%M:%S"
READ_BLOCK_BYTES = 1024
RETURN_REQUIRED_EVERY_NTH_BLOCK = 5

class ServerLog(object):
    def __init__(self, display_name, server_file, log_reader):
        self.display_name = display_name
        self.server_file = server_file
        self.log_reader = log_reader
        self.indices = self.log_reader.generate_server_log_indices(self.server_file)
        self.file_stream = open(self.server_file, "r")
        self.file_stream.seek(0,0)
        self.server_start_tm = self.log_reader.parse_dt(self.file_stream.readline())
        self.server_end_tm = self.log_reader.parse_dt(self.log_reader.read_next_line(self.file_stream, jump=0, whence=2))
        self.log_latency = LogLatency(self.log_reader)

    def destroy(self):
        try:
            if self.file_stream:
                self.file_stream.close()
            del self.display_name
            del self.server_file
            del self.log_reader
            del self.indices
            del self.file_stream
            del self.prefixes
            del self.server_start_tm
            del self.server_end_tm
            del self.log_latency
            del self.indices
            del self.file_stream
            del self.search_strings
            del self.ignore_str
            del self.is_and
            del self.is_casesensitive
            del self.slice_duration
            del self.upper_limit_check
            del self.read_all_lines
            del self.diff_it
            del self.show_it
            del self.latency_it
            del self.count_it
            del self.slice_show_count
        except Exception:
            pass

    def get_start_tm(self, start_tm="head"):
        if start_tm == "head":
            return self.server_start_tm
        else:
            return self.log_reader.parse_init_dt(start_tm, self.server_end_tm)

    def set_start_and_end_tms(self, start_tm, duration=""):
        self.process_start_tm = start_tm
        if self.process_start_tm > self.server_end_tm:
            self.process_start_tm = self.server_end_tm + self.log_reader.parse_timedelta("10")

        if duration:
            duration_tm = self.log_reader.parse_timedelta(duration)
            self.process_end_tm = self.process_start_tm + duration_tm
        if not duration or self.process_end_tm > self.server_end_tm:
            self.process_end_tm = self.server_end_tm + self.log_reader.parse_timedelta("10")

    def set_file_stream(self, system_grep=False):
        if system_grep:
            try:
                self.greped_lines = self.log_reader.grep(strs=self.search_strings, ignore_str=self.ignore_str, unique=False, file=self.server_file
                                                     , is_and=self.is_and, is_casesensitive=self.is_casesensitive).strip().split('\n')
                self.greped_lines_index = 0
            except Exception:
                #print "Error in system grep command, reading file line by line.\n"
                self.set_file_stream(system_grep=False)
        else:
            self.start_hr_tm = self.neglect_minutes_seconds_time(self.process_start_tm)
            self.server_start_hr_tm = self.neglect_minutes_seconds_time(self.server_start_tm)
            self.server_end_hr_tm = self.neglect_minutes_seconds_time(self.server_end_tm)

            if self.start_hr_tm.strftime(DT_FMT) in self.indices:
                self.file_stream.seek(self.indices[self.start_hr_tm.strftime(DT_FMT)])
            elif self.start_hr_tm < self.server_start_hr_tm:
                self.file_stream.seek(0)
            elif self.start_hr_tm > self.server_end_hr_tm:
                self.file_stream.seek(0,2)
            else:
                while(self.start_hr_tm < self.server_end_hr_tm):
                    if self.start_hr_tm.strftime(DT_FMT) in self.indices:
                        self.file_stream.seek(self.indices[self.start_hr_tm.strftime(DT_FMT)])
                        return
                    self.start_hr_tm = self.start_hr_tm + datetime.timedelta(hours=1)
                self.file_stream.seek(0,2)

    # system_grep parameter added to test and compare with system_grep. We are not using this but keeping it here for future reference.
    def set_input(self, search_strs, ignore_str="", is_and=False, is_casesensitive=True, start_tm="", duration="",
                  slice_duration="10", every_nth_slice=1, upper_limit_check="", bucket_count=3, every_nth_bucket=1,
                  read_all_lines=False, rounding_time=True, system_grep=False):
        if isinstance(search_strs, str):
            search_strs = [search_strs]
        self.search_strings=[search_str for search_str in search_strs]
        self.ignore_str = ignore_str
        self.is_and = is_and
        self.is_casesensitive = is_casesensitive
        self.slice_duration = self.log_reader.parse_timedelta(slice_duration)
        self.upper_limit_check = upper_limit_check
        self.read_all_lines = read_all_lines
        self.set_start_and_end_tms(start_tm=start_tm, duration=duration)
        self.greped_lines = []
        self.read_block = []
        self.read_block_index = 0
        self.read_block_size = 0
        self.read_block_count = 0
        self.system_grep = system_grep
        self.set_file_stream(system_grep=system_grep)
        self.diff_it = self.diff()
        self.show_it = self.show()
        latency_start_tm = self.process_start_tm
        if latency_start_tm < self.server_start_tm:
            latency_start_tm = self.server_start_tm
        self.latency_it = self.log_latency.compute_latency(self.show_it, self.search_strings[0], self.slice_duration, latency_start_tm,
                                                           self.process_end_tm, bucket_count, every_nth_bucket, arg_rounding_time=rounding_time)
        self.count_it = self.count()
        self.slice_show_count = every_nth_slice

    def read_line_block(self):
        try:
            while(True):
                self.read_block = []
                self.read_block = self.file_stream.readlines(READ_BLOCK_BYTES)
                self.read_block_count += 1
                if not self.read_block or self.read_all_lines:
                    break
                if self.search_strings:
                    one_string = " ".join(self.read_block)
                    if self.is_and:
                        if self.is_casesensitive:
                            if all(substring in one_string for substring in self.search_strings):
                                break
                        else:
                            if all(re.search(substring, one_string, re.IGNORECASE) for substring in self.search_strings):
                                break
                    else:
                        if self.is_casesensitive:
                            if any(substring in one_string for substring in self.search_strings):
                                break
                        else:
                            if any(re.search(substring, one_string, re.IGNORECASE) for substring in self.search_strings):
                                break

                    if (self.read_block_count%RETURN_REQUIRED_EVERY_NTH_BLOCK==0):
                        line = self.read_block[-1]
                        self.read_block = []
                        self.read_block.append(line)
                        break
                else:
                    break
        except Exception:
            self.read_block = []
        self.read_block_count = 0
        self.read_block_index = 0
        self.read_block_size = len(self.read_block)

    def read_line(self):
        line = None
        if self.system_grep:
            try:
                line = self.greped_lines[self.greped_lines_index]
                if line:
                    line = line + "\n"
                self.greped_lines_index += 1
            except Exception:
                pass
        else:
            if not self.read_block or self.read_block_index+1>self.read_block_size:
                self.read_line_block()
            if self.read_block and self.read_block_index+1<=self.read_block_size:
                line = self.read_block[self.read_block_index]
                self.read_block_index += 1

            # try:
            #     line = self.file_stream.readline()
            # except:
            #     pass
        return line

    def seek_back_line(self, line_lenght = 1):
        if self.system_grep:
            self.greped_lines_index -= 1
        else:
            #self.log_reader.set_next_line(file_stream=self.file_stream, jump=-(line_lenght))
            self.read_block_index -= 1

    def next_line(self, read_start_tm = None, read_end_tm = None):
        seek_back_line = False
        if not read_start_tm:
            read_start_tm = self.process_start_tm
        if not read_end_tm:
            read_end_tm = self.process_end_tm
        else:
            seek_back_line = True
        while True:
            fail = True
            line = self.read_line()
            if not line:
                return None
            line_tm = self.log_reader.parse_dt(line)
            if line_tm > read_end_tm:
                try:
                    if seek_back_line:
                        self.seek_back_line(line_lenght=len(line))
                except Exception:
                    pass
                return None
            if line_tm < read_start_tm:
                continue
            if self.read_all_lines or self.system_grep:
                return line
            if self.search_strings:
                if self.is_and:
                    if self.is_casesensitive:
                        if all(substring in line for substring in self.search_strings):
                            fail = False
                    else:
                        if all(re.search(substring, line, re.IGNORECASE) for substring in self.search_strings):
                            fail = False
                else:
                    if self.is_casesensitive:
                        if any(substring in line for substring in self.search_strings):
                            fail = False
                    else:
                        if any(re.search(substring, line, re.IGNORECASE) for substring in self.search_strings):
                            fail = False
            if fail:
                continue
            if self.ignore_str and self.ignore_str in line:
                continue
            if not fail:
                break

        return line

    def show(self):
        while True:
            tm = None
            line = self.next_line()
            if line:
                tm = self.log_reader.parse_dt(line)
            yield tm, line

    def show_iterator(self):
        return self.show_it

    def neglect_minutes_seconds_time(self, tm):
        if not tm or type(tm) is not datetime.datetime:
            return None
        return tm + datetime.timedelta(minutes=-tm.minute, seconds=-tm.second, microseconds=-tm.microsecond)

    def get_next_slice_start_and_end_tm(self, old_slice_start, old_slice_end, slice_duration, current_line_tm):
        slice_jump = 0

        if current_line_tm < old_slice_end and current_line_tm >=old_slice_start:
            return old_slice_start, old_slice_end, slice_jump
        if current_line_tm >= old_slice_end and current_line_tm < (old_slice_end + slice_duration):
            return old_slice_end, old_slice_end+slice_duration, 1
        if current_line_tm >= old_slice_end:
            d = current_line_tm-old_slice_start
            slice_jump = int((d.seconds+ 86400 * d.days)/slice_duration.seconds)
            slice_start = old_slice_start + slice_duration * slice_jump
            slice_end = slice_start + slice_duration
            return slice_start, slice_end, slice_jump
        return None, None, None

    def count(self):
        count_result = {}
        count_result[COUNT_RESULT_KEY] = {}
        slice_start = self.process_start_tm
        slice_end = slice_start + self.slice_duration
        if slice_end > self.process_end_tm:
            slice_end = self.process_end_tm
        total_count = 0
        current_slice_count = 0

        while slice_start < self.process_end_tm:
            line = self.next_line(read_start_tm=slice_start, read_end_tm=slice_end)
            if not line:
                count_result[COUNT_RESULT_KEY][slice_start.strftime(DT_FMT)] = current_slice_count
                total_count += current_slice_count
                yield slice_start, count_result
                count_result[COUNT_RESULT_KEY] = {}
                current_slice_count = 0
                slice_start = slice_end
                slice_end = slice_start + self.slice_duration
                if slice_end > self.process_end_tm:
                    slice_end = self.process_end_tm
                continue

            current_slice_count += 1

        count_result[COUNT_RESULT_KEY][TOTAL_ROW_HEADER] = total_count
        yield END_ROW_KEY, count_result

    def count_iterator(self):
        return self.count_it

    def get_value_and_diff(self, prev, slice_val):
        diff  = []
        value = []
        under_limit = True
        if self.upper_limit_check:
            under_limit = False
        if prev:
            temp = ([b - a for b, a in zip(slice_val, prev)])
            if not self.upper_limit_check or any(i >= self.upper_limit_check for i in temp):
                diff = ([b for b in temp])
                under_limit = True
        else:
            if not self.upper_limit_check or any(i >= self.upper_limit_check for i in slice_val):
                diff = ([b for b in slice_val])
                under_limit = True

        if under_limit:
            value = ([b for b in slice_val])
        return value,diff

    def diff(self):
        latencyPattern1 = '%s (\d+)'
        latencyPattern2 = '%s \(([0-9,\s]+)\)'
        latencyPattern3 = '(\d+)\((\d+)\) %s'
        latencyPattern4 = '%s \((\d+)'
        grep_str = self.search_strings[0]
        line = self.next_line()
        if line:

            value = []
            diff = []

            slice_start = self.process_start_tm
            slice_end = slice_start + self.slice_duration
            while(self.log_reader.parse_dt(line) < slice_start):
                line = self.next_line()
                if not line:
                    break

        if line:
            if self.is_casesensitive:
                m1 = re.search(latencyPattern1 % (grep_str), line)
                m2 = re.search(latencyPattern2 % (grep_str), line)
                m3 = re.search(latencyPattern3 % (grep_str), line)
                m4 = re.search(latencyPattern4 % (grep_str), line)
            else:
                m1 = re.search(latencyPattern1 % (grep_str), line, re.IGNORECASE)
                m2 = re.search(latencyPattern2 % (grep_str), line, re.IGNORECASE)
                m3 = re.search(latencyPattern3 % (grep_str), line, re.IGNORECASE)
                m4 = re.search(latencyPattern4 % (grep_str), line, re.IGNORECASE)

            while(not m1 and not m2 and not m3 and not m4):
                try:
                    line = self.next_line()
                    if not line:
                        break
                except Exception:
                    break

                if self.is_casesensitive:
                    m1 = re.search(latencyPattern1 % (grep_str), line)
                    m2 = re.search(latencyPattern2 % (grep_str), line)
                    m3 = re.search(latencyPattern3 % (grep_str), line)
                    m4 = re.search(latencyPattern4 % (grep_str), line)
                else:
                    m1 = re.search(latencyPattern1 % (grep_str), line, re.IGNORECASE)
                    m2 = re.search(latencyPattern2 % (grep_str), line, re.IGNORECASE)
                    m3 = re.search(latencyPattern3 % (grep_str), line, re.IGNORECASE)
                    m4 = re.search(latencyPattern4 % (grep_str), line, re.IGNORECASE)

        if line:
            slice_count = 0
            if (self.log_reader.parse_dt(line) >= slice_end):
                slice_start, slice_end, slice_count = self.get_next_slice_start_and_end_tm(slice_start, slice_end, self.slice_duration,self.log_reader.parse_dt(line))
                slice_count -= 1
            if slice_end > self.process_end_tm:
                slice_end = self.process_end_tm
            pattern = ""
            prev = []
            slice_val = []
            pattern_type = 0
            if m1:
                pattern = latencyPattern1 % (grep_str)
                if not slice_count%self.slice_show_count:
                    slice_val.append(int(m1.group(1)))
            elif m2:
                pattern = latencyPattern2 % (grep_str)
                if not slice_count%self.slice_show_count:
                    slice_val = map(lambda x: int(x), m2.group(1).split(","))
                pattern_type = 1
            elif m3:
                pattern = latencyPattern3 % (grep_str)
                if not slice_count%self.slice_show_count:
                    slice_val = map(lambda x: int(x), list(m3.groups()))
                pattern_type = 2
            elif m4:
                pattern = latencyPattern4 % (grep_str)
                if not slice_count%self.slice_show_count:
                    slice_val.append(int(m4.group(1)))
                pattern_type = 3

            result = {}
            result["value"] = {}
            result["diff"] = {}

            for line_tm, line in self.show_it:
                if not line:
                    break
                if line_tm >= self.process_end_tm:
                    if not slice_count%self.slice_show_count:
                        value, diff = self.get_value_and_diff(prev, slice_val)
                        if value and diff:
                            tm = slice_start.strftime(DT_FMT)
                            result["value"][tm]=value
                            result["diff"][tm]=diff
                            yield slice_start, result
                            result["value"] = {}
                            result["diff"] = {}
                            value = []
                            diff = []
                    slice_val = []
                    break

                if line_tm >= slice_end:
                    if not slice_count%self.slice_show_count:
                        value, diff = self.get_value_and_diff(prev, slice_val)
                        if value and diff:
                            tm = slice_start.strftime(DT_FMT)
                            result["value"][tm]=value
                            result["diff"][tm]=diff
                            yield slice_start, result
                            result["value"] = {}
                            result["diff"] = {}
                            value = []
                            diff = []
                        prev = slice_val
                    slice_start, slice_end, slice_count_jump = self.get_next_slice_start_and_end_tm(slice_start, slice_end, self.slice_duration,line_tm)
                    slice_count = (slice_count+slice_count_jump)%self.slice_show_count
                    slice_val = []
                    if slice_end > self.process_end_tm:
                        slice_end = self.process_end_tm

                if not slice_count%self.slice_show_count:
                    if self.is_casesensitive:
                        m = re.search(pattern, line)
                    else:
                        m = re.search(pattern, line, re.IGNORECASE)

                    if m:
                        if pattern_type == 2:
                            current = map(lambda x: int(x), list(m.groups()))
                        else:
                            current = map(lambda x: int(x), m.group(1).split(","))
                        if slice_val:
                            slice_val = ([b + a for b, a in zip(current, slice_val)])
                        else:
                            slice_val = ([b for b in current])

            if not slice_count%self.slice_show_count and slice_val:
                value, diff = self.get_value_and_diff(prev, slice_val)
                if value and diff:
                    tm = slice_start.strftime(DT_FMT)
                    result["value"][tm]=value
                    result["diff"][tm]=diff
                    yield slice_start, result

    def diff_iterator(self):
        return self.diff_it

    def latency_iterator(self):
        return self.latency_it



