// get FLOW balance of Blocto Swap FLOW<>USDT pool
pub fun main():AnyStruct{
   var v = getAccount(0xd19f554fdb83f838)
   return v.contracts.names
}
