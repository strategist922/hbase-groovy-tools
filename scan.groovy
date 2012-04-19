import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.KeyValue

def conf = HBaseConfiguration.create()

if (args.size() < 3) {
    println "Scans <table>/<family> matching <prefix> up to <limit>"
    println "Usage: ${getClass().simpleName} <table> <family> <prefix> <limit=100>"
    System.exit(1)
}


def tableName = args[0]
def family = args[1]
def prefix = args[2]
def limit = args.length > 3 ? Integer.parseInt(args[3]) : 100

def table = new HTable(conf, Bytes.toBytes(tableName))


def prefixBytes = Bytes.toBytesBinary(prefix)
def familyBytes = Bytes.toBytes(family)

println "Scanning ${tableName}/${family} with prefix ${Bytes.toStringBinary(prefixBytes)}"

def scan = new Scan(prefixBytes, new PrefixFilter(prefixBytes))
scan.addFamily(familyBytes)

def rows = 0
def scanner = table.getScanner(scan)
for (Result r : scanner) {
    println Bytes.toStringBinary(r.getRow())
    r.getColumn(familyBytes).each {KeyValue cell ->
        println "\t${Bytes.toStringBinary(cell.key)}=${Bytes.toStringBinary(cell.value)}"
    }
    if (++rows >= limit) {
        break;
    }
}
scanner.close()
table.close()

println "Found ${rows} rows."
