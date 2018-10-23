package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProductDaoImpl implements ProductDao {
    private static final String SAVE_PRODUCT_SQL = "INSERT INTO products (name, producer, price, expiration_date) VALUES (?, ?, ?, ?)";
    private static final String DELETE_PRODUCT_SQL = "DELETE FROM products WHERE id = ?";
    private static final String FIND_ALL_SQL = "SELECT * FROM products";
    private static final String FIND_ONE_SQL = "SELECT * FROM products WHERE id = ?";
    private static final String UPDATE_PRODUCT_SQL = "UPDATE products " +
            "SET name = ?, producer = ?, price = ?, expiration_date = ? WHERE id = ?";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        Objects.requireNonNull(product);
        try (Connection conn = dataSource.getConnection()) {
            saveProduct(product, conn);
        } catch (SQLException e) {
            throw new DaoOperationException("Error saving product: " + product, e);
        }
    }

    @Override
    public List<Product> findAll() {
        try (Connection conn = dataSource.getConnection()) {
            return findAllProducts(conn);
        } catch (SQLException e) {
            throw new DaoOperationException("Error fetching all products", e);
        }
    }

    @Override
    public Product findOne(Long id) {
        Objects.requireNonNull(id);
        try (Connection conn = dataSource.getConnection()) {
            return findOneProduct(id, conn);
        } catch (SQLException e) {
            throw new DaoOperationException("Error fetching one product", e);
        }
    }

    @Override
    public void update(Product product) {
        throwIfProductIdIsNull(product);
        try (Connection conn = dataSource.getConnection()) {
            updateProduct(product, conn);
        } catch (SQLException e) {
            throw new DaoOperationException("Error updating product: " + product, e);
        }
    }

    @Override
    public void remove(Product product) {
        throwIfProductIdIsNull(product);
        try (Connection conn = dataSource.getConnection()) {
            removeProduct(product, conn);
        } catch (SQLException e) {
            throw new DaoOperationException("Error removing product: " + product, e);
        }
    }

    private void saveProduct(Product product, Connection conn) throws SQLException {
        try (PreparedStatement prepStatement = conn.prepareStatement(SAVE_PRODUCT_SQL, Statement.RETURN_GENERATED_KEYS)) {
            fillInsertProductPreparedStatement(product, prepStatement);
            executeUpdate(prepStatement);
            updateProductId(product, prepStatement);
        }
    }

    private void updateProduct(Product product, Connection conn) {
        try (PreparedStatement prepStatement = conn.prepareStatement(UPDATE_PRODUCT_SQL)) {
            findOneProduct(product.getId(), conn);
            fillUpdateProductPreparedStatement(product, prepStatement);
            executeUpdate(prepStatement);
        } catch (SQLException e) {
            throw new DaoOperationException("Error executing 'update' statement", e);
        }
    }

    private void removeProduct(Product product, Connection conn) {
        try (PreparedStatement prepStatement = conn.prepareStatement(DELETE_PRODUCT_SQL)) {
            findOneProduct(product.getId(), conn);
            fillRemoveProductPreparedStatement(product, prepStatement);
            executeUpdate(prepStatement);
        } catch (SQLException e) {
            throw new DaoOperationException("Error executing 'remove' statement", e);
        }
    }

    private void updateProductId(Product product, PreparedStatement prepStatement) throws SQLException {
        ResultSet generatedKeys = prepStatement.getGeneratedKeys();
        generatedKeys.next();
        product.setId(generatedKeys.getLong(1));
    }

    private void executeUpdate(PreparedStatement prepStatement) throws SQLException {
        int affectedRows = prepStatement.executeUpdate();
        if (affectedRows < 1) {
            throw new DaoOperationException("Error executing update, affected rows: " + affectedRows);
        }
    }

    private void fillUpdateProductPreparedStatement(Product product, PreparedStatement pr) throws SQLException {
        fillInsertProductPreparedStatement(product, pr);
        pr.setLong(5, product.getId());
    }

    private void fillRemoveProductPreparedStatement(Product product, PreparedStatement pr) throws SQLException {
        pr.setLong(1, product.getId());
    }

    private List<Product> findAllProducts(Connection conn) {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(FIND_ALL_SQL)) {
            return collectProducts(rs);
        } catch (SQLException e) {
            throw new DaoOperationException("Error executing 'find all' statement", e);
        }
    }

    private Product findOneProduct(Long id, Connection conn) {
        try (PreparedStatement prepStatement = conn.prepareStatement(FIND_ONE_SQL);
             ResultSet rs = executeFindOneProductQuery(prepStatement, id)) {
            return fetchProduct(rs, id);
        } catch (SQLException e) {
            throw new DaoOperationException("Error executing 'find one' statement", e);
        }
    }

    private ResultSet executeFindOneProductQuery(PreparedStatement prepStatement, Long id) throws SQLException {
        prepStatement.setLong(1, id);
        return prepStatement.executeQuery();
    }

    private Product fetchProduct(ResultSet rs, Long id) {
        try {
            throwIfNoProductFound(rs, id);
            return parseProductRow(rs);
        } catch (SQLException e) {
            throw new DaoOperationException("Error parsing 'find one' result set", e);
        }
    }

    private List<Product> collectProducts(ResultSet rs) {
        List<Product> products = new ArrayList<>();
        try {
            while (rs.next()) {
                Product product = parseProductRow(rs);
                products.add(product);
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Error parsing 'find all' result set", e);
        }
        return products;
    }

    private void fillInsertProductPreparedStatement(Product product, PreparedStatement pr) throws SQLException {
        pr.setString(1, product.getName());
        pr.setString(2, product.getProducer());
        pr.setBigDecimal(3, product.getPrice());
        pr.setTimestamp(4, Timestamp.valueOf(product.getExpirationDate().atStartOfDay()));
    }

    private Product parseProductRow(ResultSet rs) throws SQLException {
        return Product.builder()
                .id(rs.getLong(1))
                .name(rs.getString(2))
                .producer(rs.getString(3))
                .price(rs.getBigDecimal(4))
                .expirationDate(rs.getTimestamp(5).toLocalDateTime().toLocalDate())
                .creationTime(rs.getTimestamp(6).toLocalDateTime())
                .build();
    }

    private void throwIfNoProductFound(ResultSet rs, Long id) throws SQLException {
        boolean productFound = rs.next();
        if (!productFound) {
            throw new DaoOperationException(String.format("Product with id = %d does not exist", id));
        }
    }

    private void throwIfProductIdIsNull(Product product) {
        Objects.requireNonNull(product);
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
    }
}
