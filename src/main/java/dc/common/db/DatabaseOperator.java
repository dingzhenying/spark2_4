package dc.common.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 封装数据库的操作，依赖配置数据源
 * @Description 
 * @author zhangchuanwen
 * @date 2018-06-11
 */
public class DatabaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseOperator.class);

    private QueryRunner queryRunner;
    
    public DatabaseOperator(DataSource ds){
        this.queryRunner = new QueryRunner(ds);
    }
    
    /**
     * 封装数据库查询操作
     * @param baseQuery
     * @param resultHandler
     * @param params
     * @return
     * @throws SQLException  
     * @author zhangchuanwen
     * @date 2018-06-11
     */
    public <T> T query(final String baseQuery, final ResultSetHandler<T> resultHandler, final Object... params)
            throws SQLException {
        LOG.debug("query invoke, sql:{}", baseQuery);
        try {
            return queryRunner.query(baseQuery, resultHandler, params);
        } catch (final SQLException e) {
            LOG.error("query failed", e);
            throw e;
        }
    }

    /**
     * 业务自己控制事务，具体操作在SQLTransaction的execute方法中实现
     * @param operations
     * @return
     * @throws SQLException  
     * @author zhangchuanwen
     * @date 2018-06-11
     */
    public <T> T transaction(final SQLTransaction<T> operations) throws SQLException {
        Connection conn = null;
        try {
            conn = queryRunner.getDataSource().getConnection();
            conn.setAutoCommit(false);
            final DatabaseTransactionOperator transOperator = new DatabaseTransactionOperator(conn, queryRunner);
            final T res = operations.execute(transOperator);
            conn.commit();
            return res;
        } catch (final SQLException e) {
            LOG.error("transaction error", e);
            if (conn != null){
                conn.rollback();
            }
            throw e;
        } finally {
            conn.setAutoCommit(true);
            DbUtils.closeQuietly(conn);
        }
    }

    /**
     * 封装数据库更新操作
     * @param updateClause
     * @param params
     * @return
     * @throws SQLException  
     * @author zhangchuanwen
     * @date 2018-06-11
     */
    public int update(final String updateClause, final Object... params) throws SQLException {
        LOG.debug("update invoke, sql:{}", updateClause);
        try {
            return queryRunner.update(updateClause, params);
        } catch (final SQLException e) {
            LOG.error("update failed", e);
            throw e;
        }
    }
    
    /**
     * 封装数据库批量更新操作
     * @param updateClause
     * @param params
     * @return
     * @throws SQLException  
     * @author zhangchuanwen
     * @date 2018-06-11
     */
    public int[] batch(final String batchClause, final Object[][] params) throws SQLException{
        LOG.debug("batch invoke, sql:{}", batchClause);
        try {
            return queryRunner.batch(batchClause, params);
        } catch (final SQLException e) {
            LOG.error("batch failed", e);
            throw e;
        }
    }
}
