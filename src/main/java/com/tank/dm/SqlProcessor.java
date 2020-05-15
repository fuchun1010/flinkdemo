package com.tank.dm;

import javax.annotation.Nonnull;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author tank198435163.com
 */
public class SqlProcessor {

  /**
   * sql查询
   *
   * @param sqlStatement
   * @param pstSettingOpt
   * @param converter
   * @param <T>
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public final <T> Optional<Collection<T>> queryResult(@Nonnull final String sqlStatement,
                                                       @Nonnull final Optional<Consumer<PreparedStatement>> pstSettingOpt,
                                                       @Nonnull final Function<ResultSet, T> converter)
          throws SQLException, ClassNotFoundException {

    final Set<T> results = new HashSet<>();
    T record;
    PreparedStatement pst;
    ResultSet rs;
    try (Connection conn = this.createDbConn()) {
      pst = conn.prepareStatement(sqlStatement);
      pstSettingOpt.ifPresent(pstSetting -> pstSetting.accept(pst));
      rs = pst.executeQuery();
      while (rs.next()) {
        record = converter.apply(rs);
        results.add(record);
      }
    }
    return Optional.of(results);
  }

  public final void queryResult(@Nonnull final String sqlStatement,
                                @Nonnull final Optional<Consumer<PreparedStatement>> pstSettingOpt,
                                @Nonnull final Consumer<ResultSet> converter)
          throws SQLException, ClassNotFoundException {


    PreparedStatement pst;
    ResultSet rs;
    try (Connection conn = this.createDbConn()) {
      pst = conn.prepareStatement(sqlStatement);
      pstSettingOpt.ifPresent(pstSetting -> pstSetting.accept(pst));
      rs = pst.executeQuery();
      while (rs.next()) {
        converter.accept(rs);
      }
    }
   
  }

  private Connection createDbConn() throws ClassNotFoundException, SQLException {
    Class.forName(this.driverClass);
    Connection conn = DriverManager.getConnection(url, username, password);
    return conn;
  }

  private String url = "jdbc:mysql://localhost:4000/orderCenter";
  private String driverClass = "com.mysql.cj.jdbc.Driver";
  private String username = "root";
  private String password = "";

}
