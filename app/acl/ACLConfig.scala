package acl

object ACLConfig{
  //Resources
  def DatasetResource: String = "DatasetAPI"
  //Privileges
  def ReadPrivilege:String = "read"
  def CreatePrivilege:String = "create"
  def UpdatePrivilege:String = "update"
  def DeletePrivilege:String = "delete"
  def AllPrivilege:String = "*"
}