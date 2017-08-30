./check_jmx -U service:jmx:rmi:///jndi/rmi://127.0.0.1:10000/jmxrmi -O java.lang:type=Memory -A HeapMemoryUsage -K used -I HeapMemoryUsage -J used
