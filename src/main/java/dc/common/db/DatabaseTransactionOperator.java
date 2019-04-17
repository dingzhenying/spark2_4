package dc.common.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 需要在调用方法后自己提交事务
 * @Description 
 * @author zhangchuanwen
 * @date 2018-06-08
 */
public class DatabaseTransactionOperator {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseTransactionOperator.class);

    private final Connection conn;

    private final QueryRunner queryRunner;

    public DatabaseTransactionOperator(Connection conn, QueryRunner queryRunner) {
        this.conn = conn;
        this.queryRunner = queryRunner;
    }
    
    public <T> T query(final String querySql, final ResultSetHandler<T> resultHandler, final Object... params)
            throws SQLException {
        LOG.debug("query in transaction invoke, sql:{}",  querySql);
        return this.queryRunner.query(this.conn, querySql, resultHandler, params);
    }

    public int update(final String updateClause, final Object... params) throws SQLException {
        LOG.debug("update in transaction invoke, sql:{}",  updateClause);
        return this.queryRunner.update(this.conn, updateClause, params);
    }
    
    public int[] batch(final String batchClause, final Object[][] params) throws SQLException{
        LOG.debug("batch invoke, sql:{}", batchClause);
        return queryRunner.batch(this.conn, batchClause, params);
    }
}
