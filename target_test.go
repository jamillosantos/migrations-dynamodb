package migrations_dynamodb

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jamillosantos/migrations/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Current", func() {
	var (
		ctx context.Context

		target *Target
	)

	BeforeEach(func() {
		ctx = context.Background()

		deleteAllTables(ctx)

		target = NewTarget(dynamoDBClient)
	})

	Context("Create", func() {
		When("the table does not exists", func() {
			It("should create the migrations and lock table", func() {
				Expect(target.Create(ctx)).To(Succeed())

				listTablesResponse, err := dynamoDBClient.ListTables(ctx, &dynamodb.ListTablesInput{})
				Expect(err).ToNot(HaveOccurred())

				Expect(listTablesResponse.TableNames).To(ConsistOf(
					"_migrations",
					"_migrations-lock",
				))
			})
		})

		When("the migrations table already exists but not the lock table", func() {
			It("should create the lock table", func() {
				_, err := dynamoDBClient.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String("_migrations"),
					AttributeDefinitions: []types.AttributeDefinition{
						{
							AttributeName: aws.String("id"),
							AttributeType: types.ScalarAttributeTypeS,
						},
					},
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("id"),
							KeyType:       types.KeyTypeHash,
						},
					},
					ProvisionedThroughput: &types.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(1),
						WriteCapacityUnits: aws.Int64(1),
					},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(target.Create(ctx)).To(Succeed())

				listTablesResponse, err := dynamoDBClient.ListTables(ctx, &dynamodb.ListTablesInput{})
				Expect(err).ToNot(HaveOccurred())

				Expect(listTablesResponse.TableNames).To(ConsistOf(
					"_migrations",
					"_migrations-lock",
				))
			})
		})

		When("the lock table already exists but not the migrations table", func() {
			It("should create the migrations table", func() {
				_, err := dynamoDBClient.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String("_migrations-lock"),
					AttributeDefinitions: []types.AttributeDefinition{
						{
							AttributeName: aws.String("id"),
							AttributeType: types.ScalarAttributeTypeS,
						},
					},
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("id"),
							KeyType:       types.KeyTypeHash,
						},
					},
					ProvisionedThroughput: &types.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(1),
						WriteCapacityUnits: aws.Int64(1),
					},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(target.Create(ctx)).To(Succeed())

				listTablesResponse, err := dynamoDBClient.ListTables(ctx, &dynamodb.ListTablesInput{})
				Expect(err).ToNot(HaveOccurred())

				Expect(listTablesResponse.TableNames).To(ConsistOf(
					"_migrations",
					"_migrations-lock",
				))
			})
		})

		When("the tables already exists", func() {
			It("should not error", func() {
				Expect(target.Create(ctx)).To(Succeed())
				Expect(target.Create(ctx)).To(Succeed())
			})
		})
	})

	Context("Destroy", func() {
		When("the tables exist", func() {
			It("should destroy the migrations and lock table", func() {
				Expect(target.Create(ctx)).To(Succeed())
				Expect(target.Destroy(ctx)).To(Succeed())

				listTablesResponse, err := dynamoDBClient.ListTables(ctx, &dynamodb.ListTablesInput{})
				Expect(err).ToNot(HaveOccurred())

				Expect(listTablesResponse.TableNames).To(BeEmpty())
			})
		})
	})

	Context("Add", func() {
		BeforeEach(func() {
			Expect(target.Create(ctx)).To(Succeed())
		})

		When("the migration does not exist", func() {
			It("should add the migration", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())

				ms := listMigrations(ctx)
				Expect(ms).To(HaveLen(1))
				Expect(ms[0]).To(Equal(ddbMigration{
					ID:    "1",
					Dirty: true,
				}))
			})
		})

		When("the migration already exists", func() {
			It("should fail with a ErrMigrationAlreadyExists", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.Add(ctx, "1")).To(MatchError(migrations.ErrMigrationAlreadyExists))
			})
		})
	})

	Context("Remove", func() {
		BeforeEach(func() {
			Expect(target.Create(ctx)).To(Succeed())
		})

		When("the migration does not exist", func() {
			It("should fail removing the migration", func() {
				Expect(target.Remove(ctx, "1")).To(MatchError(migrations.ErrMigrationNotFound))
			})
		})

		When("the migration exists", func() {
			It("should remove the migration", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.Remove(ctx, "1")).To(Succeed())

				ms := listMigrations(ctx)
				Expect(ms).To(HaveLen(0))
			})
		})
	})

	Context("FinishMigration", func() {
		BeforeEach(func() {
			Expect(target.Create(ctx)).To(Succeed())
		})

		When("the migration does not exist", func() {
			It("should fail removing the migration", func() {
				Expect(target.FinishMigration(ctx, "1")).To(MatchError(migrations.ErrMigrationNotFound))
			})
		})

		When("the migration exists", func() {
			It("should set dirty as false", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.FinishMigration(ctx, "1")).To(Succeed())

				ms := listMigrations(ctx)
				Expect(ms).To(HaveLen(1))
				Expect(ms[0]).To(Equal(ddbMigration{
					ID:    "1",
					Dirty: false,
				}))
			})
		})
	})

	Context("StartMigration", func() {
		BeforeEach(func() {
			Expect(target.Create(ctx)).To(Succeed())
		})

		When("the migration does not exist", func() {
			It("should fail removing the migration", func() {
				Expect(target.StartMigration(ctx, "1")).To(MatchError(migrations.ErrMigrationNotFound))
			})
		})

		When("the migration exists", func() {
			It("should set dirty as true", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.FinishMigration(ctx, "1")).To(Succeed())
				Expect(target.StartMigration(ctx, "1")).To(Succeed())

				ms := listMigrations(ctx)
				Expect(ms).To(HaveLen(1))
				Expect(ms[0]).To(Equal(ddbMigration{
					ID:    "1",
					Dirty: true,
				}))
			})
		})
	})

	Context("Done", func() {
		BeforeEach(func() {
			Expect(target.Create(ctx)).To(Succeed())
		})

		When("there is no dirty migrations", func() {
			It("should return the list of migration finished", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.FinishMigration(ctx, "1")).To(Succeed())
				Expect(target.Add(ctx, "2")).To(Succeed())
				Expect(target.FinishMigration(ctx, "2")).To(Succeed())

				ms, err := target.Done(ctx)
				Expect(err).ToNot(HaveOccurred())
				Expect(ms).To(Equal([]string{"1", "2"}))
			})
		})

		When("there is a dirty migrations", func() {
			It("should return the list of migration finished", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.FinishMigration(ctx, "1")).To(Succeed())
				Expect(target.Add(ctx, "2")).To(Succeed())

				_, err := target.Done(ctx)
				Expect(err).To(MatchError(migrations.ErrDirtyMigration))
			})
		})
	})

	Context("Current", func() {
		BeforeEach(func() {
			Expect(target.Create(ctx)).To(Succeed())
		})

		When("there is no dirty migrations", func() {
			It("should return the list of migration finished", func() {
				Expect(target.Add(ctx, "2")).To(Succeed())
				Expect(target.FinishMigration(ctx, "2")).To(Succeed())

				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.FinishMigration(ctx, "1")).To(Succeed())

				ms, err := target.Current(ctx)
				Expect(err).ToNot(HaveOccurred())
				Expect(ms).To(Equal("2"))
			})
		})

		When("there is a dirty migrations", func() {
			It("should return the list of migration finished", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.FinishMigration(ctx, "1")).To(Succeed())
				Expect(target.Add(ctx, "2")).To(Succeed())

				_, err := target.Current(ctx)
				Expect(err).To(MatchError(migrations.ErrDirtyMigration))
			})
		})
	})

	Context("Lock", func() {
		BeforeEach(func() {
			Expect(target.Create(ctx)).To(Succeed())
		})

		When("there is no dirty migrations", func() {
			It("should return the list of migration finished", func() {
				parallelCounter := 0

				for i := 1; i < 11; i++ {
					go func(i int) {
						defer GinkgoRecover()

						log.Println("Locking", i)
						u, err := target.Lock(ctx)
						Expect(err).ToNot(HaveOccurred())
						log.Println("Locked", i)
						parallelCounter++
						time.Sleep(1 * time.Second)
						defer func() {
							parallelCounter--
						}()
						defer func() {
							_ = u.Unlock(ctx)
							log.Println("Unlocked", i)
						}()
					}(i)
				}

				Consistently(func() int {
					return parallelCounter
				}, 15*time.Second).Should(Or(Equal(0), Equal(1)))
			})
		})

		When("there is a dirty migrations", func() {
			It("should return the list of migration finished", func() {
				Expect(target.Add(ctx, "1")).To(Succeed())
				Expect(target.FinishMigration(ctx, "1")).To(Succeed())
				Expect(target.Add(ctx, "2")).To(Succeed())

				_, err := target.Current(ctx)
				Expect(err).To(MatchError(migrations.ErrDirtyMigration))
			})
		})
	})
})

func deleteAllTables(ctx context.Context) {
	GinkgoHelper()

	listTablesResponse, err := dynamoDBClient.ListTables(ctx, &dynamodb.ListTablesInput{})
	Expect(err).ToNot(HaveOccurred())

	for _, tableName := range listTablesResponse.TableNames {
		_, err := dynamoDBClient.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: &tableName,
		})
		Expect(err).ToNot(HaveOccurred())
	}
}

func listMigrations(ctx context.Context) []ddbMigration {
	GinkgoHelper()

	scanResponse, err := dynamoDBClient.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String("_migrations"),
	})
	Expect(err).ToNot(HaveOccurred())

	result := make(sortMigrations, len(scanResponse.Items))
	for i, item := range scanResponse.Items {
		err = attributevalue.UnmarshalMap(item, &result[i])
		Expect(err).ToNot(HaveOccurred())
	}

	sort.Sort(result)

	return result
}

type sortMigrations []ddbMigration

func (s sortMigrations) Len() int {
	return len(s)
}

func (s sortMigrations) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

func (s sortMigrations) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
