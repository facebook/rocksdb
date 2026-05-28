Fixed WritePrepared TransactionDB cleanup after retryable commit or rollback write failures so prepared transactions remain rollbackable instead of leaving unresolved prepared state.
