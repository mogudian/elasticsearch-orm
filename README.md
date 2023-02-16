# elasticsearch-orm

为 `Elasticsearch` 提供了 `ORM` 支持，像使用数据库那样使用ES，提升开发效率

## 使用说明

- 1、集成依赖（需先将该项目源码下载并打包）

```xml
<dependency>
    <groupId>com.mogudiandian</groupId>
    <artifactId>elasticsearch-orm</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

- 2、加入配置

```properties
elasticsearch.datasource.uris=http://localhost:9200
elasticsearch.datasource.username=elastic
elasticsearch.datasource.password=YOUR_PASSWORD
elasticsearch.orm.basePackage=entity所在的包
```

- 3、编写自己的实体，相当于DO，一个实体对应一个索引

```java
@Index("user")
@Getter
@Setter
static class UserEntity implements BaseEntity {

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 头像地址
     */
    private String avatarUrl;

    /**
     * 昵称
     */
    @SearchField
    private String nick;

    /**
     * 简介
     */
    @SearchField
    private String description;

    /**
     * 接收到任务的时间 只保存60天
     */
    @LifecycleField(useCurrent = true, value = 60)
    private Date receivedTime;

    @Override
    public String entityId() {
        return userId.toString();
    }
}
```

- 4、在ES中创建entity对应的索引，必须按下面格式（ES6出的动态模板），注意如果使用了注解比如@SearchField，则不需要在property中指定，会自动生成字段

PUT /user
```json
{
  "mappings": {
    "dynamic_templates": [
      {
        "search_fields": {
          "match_mapping_type": "string",
          "match": "searchFor*",
          "mapping": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          }
        }
      },
      {
        "lifecycle_date_fields": {
          "match_mapping_type": "date",
          "match": "lifecycleFor*",
          "mapping": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
          }
        }
      },
      {
        "lifecycle_long_fields": {
          "match_mapping_type": "long",
          "match": "lifecycleFor*",
          "mapping": {
            "type": "long"
          }
        }
      },
      {
        "nested_fields": {
          "match": "nestedFor*",
          "mapping": {
            "type": "nested"
          }
        }
      },
      {
        "string_fields": {
          "match_mapping_type": "string",
          "mapping": {
            "type": "keyword"
          }
        }
      }
    ],
    "properties": {
      "userId": {
        "type": "long"
      },
      "avatarUrl": {
        "type": "keyword"
      }
    }
  }
}
```

- 5、使用工具类进行创建

```java
@Service
public class SyncUserService {

    @Autowired
    private ElasticsearchOrmClient ormClient;

    /**
     * 将数据库中的对象同步到ES
     * @param user 数据库的DO对象
     */
    public void sync(User user) {
        // 将数据库的对象转换为Entity
        UserEntity userEntity = convertDOToEntity(user);
        ormClient.add(userEntity);
    }
}
```

- 6、查询实体，有几种方式下面一一介绍
```java
// 简单方式
// 优点 使用简单
// 缺点 只能 a and b and c 只能进行等值查询(= 和 in) 并且需要在字符串中写字段名(不好refactor) 不支持嵌套 不支持模糊搜索
SimpleSearchRequest searchRequest = new SimpleSearchRequest(UserEntity.class);
searchRequest.setKeyword(keyword);
if (time1 != null || time2 != null) {
    searchRequest.setLifecycleRange(new LifecycleRange(time1, time2));
}
searchRequest.appendQueryField("userId", userId);
SearchResult<UserEntity> searchResult = client.search(searchRequest);
...
```

```java
// 复杂方式
// 优点 可以进行非等值的查询(> < between like) 可以做复杂条件例如 a and b and (c or (d and e)) 支持嵌套查询 支持模糊搜索
// 缺点 需要在字符串中写字段名(不好refactor)
ComplexSearchRequest searchRequest = new ComplexSearchRequest(UserEntity.class);
searchRequest.setKeyword(keyword);
if (time1 != null || time2 != null) {
    searchRequest.setLifecycleRange(new LifecycleRange(time1, time2));
}
if (userId != null) {
    searchRequest.appendExpression(new ConditionExpression("userId", Operator.EQUAL, userId));
}
...
```

```java
// 注解方式
// 优点 使用类和注解做参数 使用方便 并且在类中refactor非常容易 支持模糊搜索
// 缺点 只能 a and b and c 只能进行等值查询(= 和 in) 需要写查询类相对麻烦 不支持嵌套查询

@Getter
@Setter
static class UserSearchDTO implements SearchTemplate {

    /**
     * 用户ID
     */
    @Term
    private Long userId;

    /**
     * 昵称
     */
    private String nick;

    /**
     * 开始时间
     */
    @Range(name = "receivedTime", gt = true, includeLower = true)
    private Long start;

    /**
     * 结束时间
     */
    @Range(name = "receivedTime", lt = true, includeUpper = false)
    private Long end;

}

...
ComplexSearchRequest searchRequest = new ComplexSearchRequest(UserEntity.class);
UserSearchDTO userSearchDTO = new UserSearchDTO();
userSearchDTO.setStart(time1 != null ? time1.getTime() : null);
userSearchDTO.setEnd(time2 != null ? time2.getTime() : null);
userSearchDTO.setUserId(userId);
AnnotatedSearchRequest searchRequest = new AnnotatedSearchRequest(UserEntity.class, userSearchDTO);
searchRequest.setKeyword(keyword);
SearchResult<UserEntity> searchResult = client.search(searchRequest);
...
```

```java
// SQL方式
// 优点 可以进行ES支持的任意查询 学习成本低 即使不了解ES也能使用 支持模糊搜索
// 缺点 不支持转义字符 执行效率相对低(要parse and explain) 对于null处理不好(语义不明确) 对于日期类型处理不好(只能按固定格式)
String sql = "where (nick like ? or description like ?) and receivedTime between ? and ? order by receivedTime desc";
SqlSearchRequest searchRequest = new SqlSearchRequest(UserEntity.class, sql);
searchRequest.setParameters(keyword, keyword, time1 != null ? time1.getTime() : null, time2 != null ? time2.getTime() : null);
SearchResult<UserEntity> searchResult = client.search(searchRequest);
...
```

## 参考
SQL部分的实现参考（抄袭）了 [NLPchina/elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql)

## 依赖三方库

| 依赖                                     | 版本号           | 说明  |
|----------------------------------------|---------------|-----|
| spring-boot                            | 2.3.4.RELEASE |     |
| spring-boot-starter-data-elasticsearch | 2.3.4.RELEASE |     |
| fastjson                               | 1.2.73        |     |
| commons-lang3                          | 3.11          |     |
| guava                                  | 29.0-jre      |     |
| druid                                  | 1.1.16        |     |
| lombok                                 | 1.18.16       |     |

## 使用前准备

- [Maven](https://maven.apache.org/) (构建/发布当前项目)
- Java 8 ([Download](https://adoptopenjdk.net/releases.html?variant=openjdk8))

## 构建/安装项目

使用以下命令:

`mvn clean install`

## 发布项目

修改 `pom.xml` 的 `distributionManagement` 节点，替换为自己在 `settings.xml` 中 配置的 `server` 节点，
然后执行 `mvn clean deploy`

举例：

`settings.xml`

```xml

<servers>
    <server>
        <id>snapshots</id>
        <username>yyy</username>
        <password>yyy</password>
    </server>
    <server>
        <id>releases</id>
        <username>xxx</username>
        <password>xxx</password>
    </server>
</servers>
```

`pom.xml`

```xml

<distributionManagement>
    <snapshotRepository>
        <id>snapshots</id>
        <url>http://xxx/snapshots</url>
    </snapshotRepository>
    <repository>
        <id>releases</id>
        <url>http://xxx/releases</url>
    </repository>
</distributionManagement>
```
