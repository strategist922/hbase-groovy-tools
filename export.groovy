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
    println "Scans <table>/<family> matching <prefix> up to <limit> exporting CSV containing row key, email md5 hash, email"
    println "Usage: ${getClass().simpleName} <table> <family> <prefix> <limit=100>"
    println "Example: ./hbase_groovy.sh export.groovy contacts_graph_20121231 c '\\x00\\xFFfacebook\\x09' 0 | gzip > /mnt/core/targusexport/facebook-export.tsv.gz"
    // Example: ./hbase_groovy.sh export.groovy contacts_graph_20121231 c '\x00\xFFfacebook\x09' 0 | gzip > /mnt/core/targusexport/facebook-export.tsv.gz


    System.exit(1)
}


def tableName = args[0]
def family = args[1]
def prefix = args[2]
def limit = args.length > 3 ? Integer.parseInt(args[3]) : 100

def table = new HTable(conf, Bytes.toBytes(tableName))

def md5 = MessageDigest.getInstance('MD5')

/* The prefix consists of the contactFieldAbbreviation and field value delimited by 0xFF.  A list of valid contactFieldAbbriviations can be found in the ContactFieldFactory class 
   in the identibase project.  The value of account is further divided into account type and value seperated by tab (0x09) so adding the trailing 0x09 delimiter prevents scanning 
   into other account types inadvertently although this shouldn't be an issue with current account type values.  */

def prefixBytes = Bytes.toBytesBinary(prefix)
// column family: eg 'c'
def familyBytes = Bytes.toBytes(family)

System.err.println "Scanning ${tableName}/${family} with prefix ${Bytes.toStringBinary(prefixBytes)}"

def filter = new FilterList(FilterList.Operator.MUST_PASS_ALL)

// \\0x02 - email contact field abbriviation, the column name contains the
// email address 
filter.addFilter new ColumnPrefixFilter(Bytes.toBytesBinary('\\x02'))

def scan = new Scan(prefixBytes, filter)
scan.addFamily(familyBytes)

def rows = 0
def edges = 0
def scanner = table.getScanner(scan)
for (Result r : scanner) {
    r.getFamilyMap(familyBytes).each {byte[] key, byte[] value ->
        try {
            def rowkey = Bytes.toString(r.row).split("\t")

            if(rowkey.size() < 2) {
                System.err.println "Invalid row key: $rowkey".toString()
                return 
            }

            def account = rowkey[1]	// 0 = account type, 1 = account value
            // email stored in column name
            def email = Bytes.toString(key)?.substring(2)?.trim()?.toLowerCase()
            // hashed value
            def hashed = new BigInteger(1, md5.digest(email.bytes)).toString(16).padLeft(32, '0')
            // write output ot file
            println "${account}\t${hashed}\t${email}".toString()
            if (++edges % 1000 == 0) {
                System.err.println "${edges}\t${account}\t${email}".toString()
            }
        } catch (Throwable ex) {
            System.err.println "${new Date()} EXCEPTION".toString()
                ex.printStackTrace(System.err)
        }
    }
    if (++rows >= limit && limit > 0) {
        break;
    }
}
scanner.close()
table.close()

System.err.println "Found ${rows} rows and ${edges} edges."
