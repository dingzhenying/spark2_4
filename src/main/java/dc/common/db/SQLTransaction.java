package dc.common.db;

import java.sql.SQLException;

public interface SQLTransaction<T> {
    public T execute(DatabaseTransactionOperator transionOperator) throws SQLException;
}
