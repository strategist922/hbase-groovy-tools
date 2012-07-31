import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.KeyValue
import org.apache.commons.codec.binary.Hex

def conf = HBaseConfiguration.create()

if (args.size() < 2) {
    println "Deletes row matching <row> bytes"
    println "Usage: ${getClass().simpleName} <table> <row>"
    System.exit(1)
}


def tableName = args[0]
def row = args[1]

def table = new HTable(conf, Bytes.toBytes(tableName))

println "Deleting ${tableName}/${row}"

def delete = new Delete(Bytes.toBytesBinary(row))

table.delete(delete)
table.close()