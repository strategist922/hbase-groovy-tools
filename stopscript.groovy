lastLine = null
System.in.eachLine { it -> 
 c = it.charAt(0)
 if(c < lastLine) System.exit(0)
 lastLine = c
 println it
}
