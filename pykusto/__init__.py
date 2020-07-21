# Allows importing externally-facing classes without specifying submodules.
# e.g. "from pykusto import PyKustoClient" instead of "from pykusto.client import PyKustoClient"
# Also allows for a convenient list of all externally-facing classes as the autocomplete of "import pykusto."
# "import *" does not import names which start with an underscore

from pykusto.client import *
from pykusto.enums import *
from pykusto.expressions import *
from pykusto.functions import *
from pykusto.query import *

name = "pykusto"

