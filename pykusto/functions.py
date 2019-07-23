from pykusto.expressions import NumberType, NumberExpression
from pykusto.utils import KQL


# Scalar functions
def abs(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('abs({})'.format(expr)))


def acos(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('acos({})'.format(expr)))


def ago(): return  # TODO


def array_concat(): return


def array_iif(): return


def array_length(): return  # TODO


def array_slice(): return


def array_split(): return


def asin(): return


def atan(self): return


def atan2(self): return


def base64_decode_toarray(self): return


def base64_decode_tostring(self): return


def base64_encode_tostring(self): return


def bag_keys(self): return


def beta_cdf(self): return


def beta_inv(self): return


def beta_pdf(self): return


def bin(self): return  # TODO


def bin_at(self): return  # TODO


def bin_auto(self): return  # TODO


def binary_and(self): return


def binary_not(self): return


def binary_or(self): return


def binary_shift_left(self): return


def binary_shift_right(self): return


def binary_xor(self): return


def case(self): return  # TODO


def ceiling(self): return  # TODO


def coalesce(self): return


def column_ifexists(self): return


def cos(self): return


def cot(self): return


def countof(self): return  # TODO


def current_cluster_endpoint(self): return


def current_cursor(self): return


def current_database(self): return


def current_principal(self): return


def cursor_after(self): return


def cursor_before_or_at(self): return


def cursor_current(self): return


def datetime_add(self): return  # TODO


def datetime_part(self): return  # TODO


def datetime_diff(self): return  # TODO


def dayofmonth(self): return  # TODO


def dayofweek(self): return  # TODO


def dayofyear(self): return  # TODO


def dcount_hll(self): return  # TODO


def degrees(self): return


def endofday(self): return  # TODO


def endofmonth(self): return  # TODO


def endofweek(self): return  # TODO


def endofyear(self): return  # TODO


def estimate_data_size(self): return


def exp(self): return  # TODO


def exp10(self): return  # TODO


def exp2(self): return  # TODO


def extent_id(self): return


def extent_tags(self): return


def extract(self): return  # TODO


def extract_all(self): return


def extractjson(self): return  # TODO


def floor(self): return  # TODO


def format_datetime(self): return  # TODO


def format_timespan(self): return  # TODO


def gamma(self): return


def getmonth(self): return  # TODO


def gettype(self): return  # TODO


def getyear(self): return  # TODO


def hash(self): return  # TODO


def hash_sha256(self): return  # TODO


def hll_merge(self): return


def hourofday(self): return  # TODO


def iif(self): return  # TODO


def indexof(self): return


def indexof_regex(self): return


def ingestion_time(self): return


def isascii(self): return


def isempty(self): return  # TODO


def isfinite(self): return


def isinf(self): return  # TODO


def isnan(self): return  # TODO


def isnotempty(self): return  # TODO


def isnotnull(self): return  # TODO


def isnull(self): return  # TODO


def isutf8(self): return


def log(self): return  # TODO


def log10(self): return  # TODO


def log2(self): return  # TODO


def loggamma(self): return


def make_datetime(self): return  # TODO


def make_string(self): return  # TODO


def make_timespan(self): return  # TODO


def max_of(self): return


def min_of(self): return


def monthofyear(self): return  # TODO


def new_guid(self): return  # TODO


def now(self): return  # TODO


def pack(self): return


def pack_all(self): return


def pack_array(self): return


def pack_dictionary(self): return


def parse_csv(self): return


def parse_ipv4(self): return


def parse_json(self): return  # TODO


def parse_path(self): return


def parse_url(self): return


def parse_urlquery(self): return


def parse_user_agent(self): return


def parse_version(self): return


def parse_xml(self): return


def percentile_tdigest(self): return  # TODO


def percentrank_tdigest(self): return  # TODO


def pi(self): return  # TODO


def pow(self): return  # TODO


def radians(self): return


def rand(self): return


def range(self): return


def rank_tdigest(self): return


def repeat(self): return


def replace(self): return  # TODO


def reverse(self): return


def round(self): return  # TODO


def series_add(self): return


def series_decompose(self): return


def series_decompose_anomalies(self): return


def series_decompose_forecast(self): return


def series_divide(self): return


def series_equals(self): return


def series_fill_backward(self): return


def series_fill_const(self): return


def series_fill_forward(self): return


def series_fill_linear(self): return


def series_fir(self): return


def series_fit_2lines(self): return


def series_fit_2lines_dynamic(self): return


def series_fit_line(self): return


def series_fit_line_dynamic(self): return


def series_greater(self): return


def series_greater_equals(self): return


def series_iir(self): return


def series_less(self): return


def series_less_equals(self): return


def series_multiply(self): return


def series_not_equals(self): return


def series_outliers(self): return


def series_periods_detect(self): return


def series_periods_validate(self): return


def series_seasonal(self): return


def series_stats(self): return


def series_stats_dynamic(self): return


def series_subtract(self): return


def set_difference(self): return


def set_intersect(self): return


def set_union(self): return


def sign(self): return  # TODO


def sin(self): return


def split(self): return


def sqrt(self): return  # TODO


def startofday(self): return  # TODO


def startofmonth(self): return  # TODO


def startofweek(self): return  # TODO


def startofyear(self): return  # TODO


def strcat(self): return  # TODO


def strcat_array(self): return  # TODO


def strcat_delim(self): return  # TODO


def strcmp(self): return  # TODO


def string_size(self): return  # TODO


def strlen(self): return  # TODO


def strrep(self): return  # TODO


def substring(self): return  # TODO


def tan(self): return


def tdigest_merge(self): return


def tobool(self): return  # TODO


def todatetime(self): return  # TODO


def todecimal(self): return  # TODO


def todouble(self): return  # TODO


def todynamic(self): return  # TODO


def toguid(self): return  # TODO


def tohex(self): return  # TODO


def toint(self): return  # TODO


def tolong(self): return  # TODO


def tolower(self): return  # TODO


def toreal(self): return  # TODO


def tostring(self): return  # TODO


def totimespan(self): return  # TODO


def toupper(self): return  # TODO


def to_utf8(self): return


def translate(self): return


def treepath(self): return


def trim(self): return  # TODO


def trim_end(self): return


def trim_start(self): return


def url_decode(self): return  # TODO


def url_encode(self): return  # TODO


def weekofyear(self): return  # TODO


def welch_test(self): return


def zip(self): return  # TODO


# aggregative functions
def any(self):  # TODO
    return


def arg_max(self):  # TODO
    return


def arg_min(self):  # TODO
    return


def avg(self):  # TODO
    return


def avgif(self):  # TODO
    return


def buildschema(self):
    return


def count(self):  # TODO
    return


def countif(self):  # TODO
    return


def dcount(self):  # TODO
    return


def dcountif(self):  # TODO
    return


def hll(self):  # TODO
    return


def hll_merge(self):
    return


def make_bag(self):
    return


def make_list(self):  # TODO
    return


def make_set(self):  # TODO
    return


def max(self):  # TODO
    return


def min(self):  # TODO
    return


def percentiles(self):  # TODO
    return


def stdev(self):  # TODO
    return


def stdevif(self):  # TODO
    return


def stdevp(self):  # TODO
    return


def sum(self):  # TODO
    return


def sumif(self):  # TODO
    return


def tdigest(self):
    return


def tdigest_merge(self):
    return


def variance(self):  # TODO
    return


def varianceif(self):  # TODO
    return


def variancep(self):  # TODO
    return


# Join functions
def left(self):  # TODO
    return


def right(self):  # TODO
    return


# More functions
def disticnt(self):  # TODO
    return


def col(self):  # TODO
    return
