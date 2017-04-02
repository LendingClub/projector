## 1.4.1 - 2017-04-02
* Build fix to work around NeoRx bintray approval

## 1.4.0 - 2017-04-02
* Change to NeoRx 2.x & Neo4j BOLT

## 1.3.1 - 2017-03-28
* Changed unique identifier for docker to mercatorId
* Added DockerManager label

## 1.3.0 - 2017-03-27
* Added Docker support
* Added UCS Support
* Added New Relic Support
* Minor Refactoring

## 1.2.0
* added ScannerContext
* fix tag aws removal for tags with special characters

## 1.1.1
* tags were not scanned on individual ELB scan requests

## 1.1.0
* minor breaking api changes
* upgrade slf4j and logback
* add AWSScannerGroup

## 1.0.6
* fix bug in ASG scanning

## 1.0.5
* add schema/constraint management
* add security group arn

## 1.0.4
* fix account id lookup if version is not specified

## 1.0.3
* reduce visibility of  doScan() methods from public to protected
* remove problematic member withAutoScalingGroupNames()

## 1.0.2

* Upgrade to AWS SDK 1.11.93
* Allow AWS Region to be specified as a String

## 1.0.1

* Initial release
