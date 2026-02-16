Status of the 'HTTP_HOST' header setting within the global server context on the host	

The 'HTTP_HOST' parameter is part of a global server context setting requiring all requests to use host named headers across the website.  As there are several automated exploits using IP based headers, this setting should be carefully configured according to the current security policies and needs of the business. 
NOTE: This control should be used with CID#7638.	Apache HTTP Server 2.4.x	HIGH	4	The following List String value(s) X indicate the current RewriteCond {HTTP_HOST} setting within the global server context on the host. NOTE: All requests should contain 'HTTP_HOST' headers, while all IP-based requests should be denied; according to the CIS Benchmark.

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    %{HTTP_HOST}.*((xn--)?[a-z0-9]+(-[a-z0-9]+)*(\\.)|(\.))+[a-z]{2,}.*
    OR, any of the selected values below:
    [ ] Setting not found

Status of the 'REQUEST_URI' header setting within the global server context on the host	

The 'REQUEST_URI' parameter is part of a global server context setting requiring all requests to avoid IP based headers across the website.  As there are several automated exploits using IP based headers, this setting should be carefully configured according to the current security policies and needs of the business. 
NOTE: This control should be used with CID#7639.	Apache HTTP Server 2.4.x	HIGH	4	The following List String value(s) X indicate the current RewriteCond {REQUEST_URI} setting within the global server context on the host. NOTE: All IP-based header requests should be denied according to the CIS Benchmark.

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    REQUEST_URI
    OR, any of the selected values below:
    [ ] Setting not found

Status of the 'Listen' directive in the Apache configuration file on the host	

The 'Listen' directive identifies port numbers and ip addresses that Apache will listen to for requests. As there are risks to identifying no IP addresses, IP addresses with zeroes, and defining too many IP addresses for the 'Listen' directive, this directive should be configured according to the needs of the business.	Apache HTTP Server 2.4.x	MEDIUM	3	The following List String value(s) X indicate the current Listen Directive settings within the Apache configuration file on the host. NOTE: No empty 'Listen' directive should be permitted according to the CIS Benchmark. The following output should be reviewed, edited (if needed), and approved.

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    ^(?!(0\.0\.0\.0)|(::ffff:0\.0\.0\.0))(.+):[1-9]\d*$
    OR, any of the selected values below:
    [ ] Setting not found


Status of Permissions-Policy header	

The Permissions-Policy HTTP header replaces the existing Feature-Policy header for controlling delegation of permissions and powerful features. The header uses a structured syntax, and allows sites to more tightly restrict which origins can be granted access to features.This must be configured as per the needs of the business.	Apache HTTP Server 2.4.x	MEDIUM	3	The following List String value(s) X indicate the status of Permissions-Policy header of the $HTTP_prefix/conf/httpd.conf file(s).

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    .+
    OR, any of the selected values below:
    [ ] Setting not found


Status of Referrer-Policy header	

The Referrer-Policy HTTP header controls how much referrer information (sent with the Referer header) should be included with requests.This must be configured as per the needs of the business.	Apache HTTP Server 2.4.x	HIGH	4	The following List String value(s) X indicate the status of Referrer-Policy header of the $HTTP_prefix/conf/httpd.conf file(s).

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    .+
    OR, any of the selected values below:
    [ ] Setting not found


Status of the number of effective SecRule	

The OWASP (Open Web Application Security Project) CRS (Core Rule Set) is a broadly used open source set of generic attack detection rules which helps to protect the web server / application. It gives the base protection against HTTP DoS, common Web Attacks, Trojan, Real-time block list etc. This reduce the surface attack area of web server / application. So, this setting should be configured as per the needs of the business.	Apache HTTP Server 2.4.x	HIGH	4	The following Integer value X indicates the total number of SecRules applied on the host.

    * * * * * Expected Value(s) * * * * *
    greater than
    0
    OR, any of the selected values below:
    [ ] No SecRule found


Status of 'inbound_anomaly_score_threshold' setting within the OWASP ModSecurity ruleset	

The 'inbound_anomaly_score_threshold' defines the anomaly score at which the incoming transaction/request is blocked. The aggregate of the anomalies occurred in the incoming transaction/request needs to be higher than the 'inbound_anomaly_score_threshold' to deny the transaction/request. The Anomaly Threshold values set a limit so that traffic is not blocked until the threshold is exceeded. The OWASP (Open Web Application Security Project) CRS (Core Rule Set) is a broadly used open source set of generic attack detection rules which helps to protect the web server/application. It Provides the baseline protections against HTTP DoS, common Web Attacks, Trojan, Real-time block list by reducing the surface attack area of web server/application. Lower thresholds provide better security but can lead to denying non-malicious transactions (false positives) thus, this should be configured in accordance with the needs of the business.	Apache HTTP Server 2.4.x	MEDIUM	3	The following List string value(s) X indicates the status of 'inbound_anomaly_score_threshold' setting for modsecurity on the host.

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    inbound_anomaly_score_threshold=[1-5]
    OR, any of the selected values below:
    [ ] Setting not found


Status of 'outbound_anomaly_score_threshold' setting within the OWASP ModSecurity ruleset	

The 'outbound_anomaly_score_threshold' setting defines the anomaly score at which the outgoing transaction/response is blocked. The aggregate of the anomalies occurred in the outbound transaction/response needs to be higher than the 'outbound_anomaly_score_threshold' to deny the transaction/response. The Anomaly Threshold values set a limit so that traffic is not blocked until the threshold is exceeded. The OWASP (Open Web Application Security Project) CRS (Core Rule Set) is a broadly used open source set of generic attack detection rules which helps to protect the web server/application. It Provides the baseline protections against HTTP DoS, common Web Attacks, Trojan, Real-time block list by reducing the surface attack area of web server/application. Lower thresholds provide better security but can lead to denying non-malicious transactions (false positives) thus, this should be configured in accordance with the needs of the business.	Apache HTTP Server 2.4.x	MEDIUM	3	The following List string value(s)X indicates the status of 'outbound_anomaly_score_threshold' setting for modsecurity on the host.

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    outbound_anomaly_score_threshold=[1-4]
    OR, any of the selected values below:
    [ ] Setting not found


Status of 'paranoia_level' setting within the OWASP ModSecurity ruleset	

The 'paranoia_level' setting allows users to disable certain rules in order to reduce the number of false positives that they may encounter. The Anomaly Threshold values set a limit so that traffic is not blocked until the threshold is exceeded. The OWASP (Open Web Application Security Project) CRS (Core Rule Set) is a broadly used open source set of generic attack detection rules which helps to protect the web server/application. It Provides the baseline protections against HTTP DoS, common Web Attacks, Trojan, Real-time block list by reducing the surface attack area of web server/application. It is recommended to define suitable paranoia level according to the security level of the service in question and configure this setting according to the needs of the business.	Apache HTTP Server 2.4.x	MEDIUM	3	The following List string value(s)X indicates the status of 'paranoia_level' setting for modsecurity on the host.

    * * * * * Expected Value(s) * * * * *
    matches regular expression list
    (paranoia_level=([1-9]|\d\d+))$
    OR, any of the selected values below:
    [ ] Setting not found


Status of the 'Options' setting within the 'Alias directive directory' on the host	

The 'Options' directive establishes which features are available in a specific directory such as MultiViews, FollowSymLinks, and Indexes.  As there are several cross site scripting, buffer overflow, and privilege escalation exploits targeting default content, the 'Options' directive within the 'Alias' directive should be carefully configured according to the needs of the business.  NOTE: The 'Options' directive should be commented out according to the CIS Benchmark.	Apache HTTP Server 2.4.x	MEDIUM	3	The following List String value(s) X indicate the current value of the Options setting within the Alias directive directory on the host.

    * * * * * Expected Value(s) * * * * *
    does not contain regular expression list
    .+
    OR, any of the selected values below:
    [x] Setting not found
    [x] Alias containg icons not found


Status of the 'AllowOverride' directive within the 'Alias' directive on the host

The 'AllowOverride' directive lists the type of directives allowed in .htaccess files . As there are several cross site scripting, buffer overflow, and privilege escalation exploits targeting default content, the 'AllowOverride' directive within the 'Alias' directive should be carefully configured according to the needs of the business.  NOTE: The 'AllowOverride' directive should be set to a value of 'None', according to the CIS Benchmark.	Apache HTTP Server 2.4.x	MEDIUM	3	The following List String value(s) X indicate the current setting for the AllowOverride setting within the Alias Directive directory on the host. NOTE: The 'AllowOverride' setting should be commented out according to the CIS Benchmark.

    * * * * * Expected Value(s) * * * * *
    does not contain regular expression list
    .+
    OR, any of the selected values below:
    [x] Setting not found
    [x] Alias containg icons not found


Status of the 'httpd_t' type/domain's Permissive mode setting in SELinux	

SELinux allows configuring Permissive mode for individual processes and process types running in domains selectively.   This feature allows a system to run with SELinux enabled, while permitting all accesses that a specific application was trying to perform.  This can be helpful in troubleshooting issues and testing for the proper function of a web application, because all such accesses will be recoded in logs for that process.  However, it is important to note that a SELinux-enabled system running in permissive mode is not protected by SELinux.  Hence, the use of Permissive mode should be restricted as appropriate to the needs of the business.	Apache HTTP Server 2.4.x	HIGH	4	The following List String value(s) X indicate whether the 'httpd_t' process type is configured to run in Permissive Mode in SELinux.  An empty result i.e., no output, indicates that the permissive mode is not configured for this process type.

    * * * * * Expected Value(s) * * * * *
    does not contain regular expression list
    permissive
    OR, any of the selected values below:
    [x] Setting not found
