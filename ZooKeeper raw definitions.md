/**
 * Encodes a composite transaction.  In the wire format, each transaction
 * consists of a single MultiHeader followed by the appropriate request.
 * Each of these MultiHeaders has a type which indicates
 * the type of the following transaction or a negative number if no more transactions
 * are included.
 */
 
 /**
  * Represents a single operation in a multi-operation transaction.  Each operation can be a create, update
  * or delete or can just be a version check.
  *
  * Sub-classes of Op each represent each detailed type but should not normally be referenced except via
  * the provided factory methods.
  *
  * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode)
  * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode, org.apache.zookeeper.AsyncCallback.StringCallback, Object)
  * @see ZooKeeper#delete(String, int)
  * @see ZooKeeper#setData(String, byte[], int)
  */
  
  
   public void serialize(OutputArchive archive, String tag) throws IOException {
          archive.startRecord(this, tag);
          int index = 0 ;
          for (Op op : ops) {
              MultiHeader h = new MultiHeader(op.getType(), false, -1);
              h.serialize(archive, tag);
              switch (op.getType()) {
                 case ZooDefs.OpCode.create:
                      op.toRequestRecord().serialize(archive, tag);
                      break;
                  case ZooDefs.OpCode.delete:
                      op.toRequestRecord().serialize(archive, tag);
                      break;
                  case ZooDefs.OpCode.setData:
                      op.toRequestRecord().serialize(archive, tag);
                      break;
                  case ZooDefs.OpCode.check:
                      op.toRequestRecord().serialize(archive, tag);
                      break;
                  default:
                      throw new IOException("Invalid type of op");
              }
          }
          new MultiHeader(-1, true, -1).serialize(archive, tag);
          archive.endRecord(this, tag);
      }
      
    /**
       * Executes multiple ZooKeeper operations or none of them.
       * <p>
       * On success, a list of results is returned.
       * On failure, an exception is raised which contains partial results and
       * error details, see {@link KeeperException#getResults}
       * <p>
       * Note: The maximum allowable size of all of the data arrays in all of
       * the setData operations in this single request is typically 1 MB
       * (1,048,576 bytes). This limit is specified on the server via
       * <a href="http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#Unsafe+Options">jute.maxbuffer</a>.
       * Requests larger than this will cause a KeeperException to be
       * thrown.
       *
       * @param ops An iterable that contains the operations to be done.
       * These should be created using the factory methods on {@link Op}.
       * @return A list of results, one for each input Op, the order of
       * which exactly matches the order of the <code>ops</code> input
       * operations.
       * @throws InterruptedException If the operation was interrupted.
       * The operation may or may not have succeeded, but will not have
       * partially succeeded if this exception is thrown.
       * @throws KeeperException If the operation could not be completed
       * due to some error in doing one of the specified ops.
       *
       * @since 3.4.0
       */
           
           
   protected List<OpResult> multiInternal(MultiTransactionRecord request)
           throws InterruptedException, KeeperException {
           RequestHeader h = new RequestHeader();
           h.setType(ZooDefs.OpCode.multi);
           MultiResponse response = new MultiResponse();
           ReplyHeader r = cnxn.submitRequest(h, request, response, null);
           if (r.getErr() != 0) {
               throw KeeperException.create(KeeperException.Code.get(r.getErr()));
           }
   
           List<OpResult> results = response.getResultList();
           
           ErrorResult fatalError = null;
           for (OpResult result : results) {
               if (result instanceof ErrorResult && ((ErrorResult)result).getErr() != KeeperException.Code.OK.intValue()) {
                   fatalError = (ErrorResult) result;
                   break;
               }
           }
   
           if (fatalError != null) {
               KeeperException ex = KeeperException.create(KeeperException.Code.get(fatalError.getErr()));
               ex.setMultiResults(results);
               throw ex;
           }
   
           return results;
       }
       
       Déserialisation de la multiResponse
       
   public void deserialize(InputArchive archive, String tag) throws IOException {
           results = new ArrayList<OpResult>();
   
           archive.startRecord(tag);
           MultiHeader h = new MultiHeader();
           h.deserialize(archive, tag);
           while (!h.getDone()) {
               switch (h.getType()) {
                   case ZooDefs.OpCode.create:
                       CreateResponse cr = new CreateResponse();
                       cr.deserialize(archive, tag);
                       results.add(new OpResult.CreateResult(cr.getPath()));
                       break;
   
                   case ZooDefs.OpCode.delete:
                       results.add(new OpResult.DeleteResult());
                       break;
   
                   case ZooDefs.OpCode.setData:
                       SetDataResponse sdr = new SetDataResponse();
                       sdr.deserialize(archive, tag);
                       results.add(new OpResult.SetDataResult(sdr.getStat()));
                       break;
   
                   case ZooDefs.OpCode.check:
                       results.add(new OpResult.CheckResult());
                       break;
   
                   case ZooDefs.OpCode.error:
                       //FIXME: need way to more cleanly serialize/deserialize exceptions
                       ErrorResponse er = new ErrorResponse();
                       er.deserialize(archive, tag);
                       results.add(new OpResult.ErrorResult(er.getErr()));
                       break;
   
                   default:
                       throw new IOException("Invalid type " + h.getType() + " in MultiResponse");
               }
               h.deserialize(archive, tag);
           }
           archive.endRecord(tag);
       }