import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter
import org.apache.hadoop.hbase.filter.FilterList
import java.security.MessageDigest

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

def md5 = MessageDigest.getInstance('MD5')


def prefixBytes = Bytes.toBytesBinary(prefix)
def familyBytes = Bytes.toBytes(family)

System.err.println "Scanning ${tableName}/${family} with prefix ${Bytes.toStringBinary(prefixBytes)}"

def filter = new FilterList(FilterList.Operator.MUST_PASS_ALL)
//filter.addFilter new PrefixFilter(prefixBytes)
filter.addFilter new ColumnPrefixFilter(Bytes.toBytesBinary('\\x02'))

def scan = new Scan(prefixBytes, filter)
scan.addFamily(familyBytes)

def rows = 0
def edges = 0
def scanner = table.getScanner(scan)
for (Result r : scanner) {
    r.getFamilyMap(familyBytes).each {byte[] key, byte[] value ->
	try {
	def twitter = Bytes.toString(r.row).split("\t")[1]	
	def email = Bytes.toString(key)?.substring(2)?.trim()?.toLowerCase()
	def hashed = new BigInteger(1, md5.digest(email.bytes)).toString(16).padLeft(32, '0')
        println "${twitter}\t${hashed}\t${email}"
        if (++edges % 1000 == 0) {
    	    System.err.println "${edges}\t${twitter}\t${email}"
        }
	} catch (Throwable ex) {
	    System.err.println "${new Date()} EXCEPTION", ex
	}
    }
    if (++rows >= limit && limit > 0) {
        break;
    }
}
scanner.close()
table.close()

System.err.println "Found ${rows} rows and ${edges} edges."
