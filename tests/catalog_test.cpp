#include <gtest/gtest.h>
#include "catalog/catalog_manager.h"

TEST(CatalogManagerTest, CreateTableResultHasExpectedFields) {
    CatalogManager::CreateTableResult result{true, ""};
    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.error_msg.empty());
}

TEST(CatalogManagerTest, DropResultHasExpectedFields) {
    CatalogManager::DropResult result{false, "table not found"};
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_msg, "table not found");
}

TEST(CatalogManagerTest, AlterResultHasExpectedFields) {
    CatalogManager::AlterResult result{true, ""};
    EXPECT_TRUE(result.success);
}
