import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Delete

def conf = HBaseConfiguration.create()

if (args.size() == 0) {
    println "Deletes rows from .META. matching a given prefix. USE WITH CARE!!!"
    println "Usage: ${getClass().simpleName} [row key prefix]."
    System.exit(1)
}

def prefix = args[0]
assert prefix, "A .META. rowkey prefix is required"

def metaTable = new HTable(conf, HConstants.META_TABLE_NAME)

def prefixBytes = Bytes.toBytes(prefix)

def deletes = []
metaTable.getScanner(new Scan(prefixBytes, new PrefixFilter(prefixBytes))).each {Result r ->
    deletes << new Delete(r.row)
}

if (deletes) {
    println "Found ${deletes.size()} rows to delete:"
    deletes.each {
        println Bytes.toString(it.row)
    }
    print "Are you sure? "
    System.in.withReader {reader ->
        def response = reader.readLine()
        if (response =~ /[yY].*/) {
            println "Deleting..."
            metaTable.delete(deletes)
        }
    }
}
