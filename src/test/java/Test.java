import com.mogudiandian.elasticsearch.orm.DefaultElasticsearchOrmClient;
import com.mogudiandian.elasticsearch.orm.ElasticsearchOrmClient;
import com.mogudiandian.elasticsearch.orm.core.annotation.Index;
import com.mogudiandian.elasticsearch.orm.core.annotation.LifecycleField;
import com.mogudiandian.elasticsearch.orm.core.annotation.SearchField;
import com.mogudiandian.elasticsearch.orm.core.request.SearchRequest;
import com.mogudiandian.elasticsearch.orm.core.request.api.LifecycleRange;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.AnnotatedSearchRequest;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.SearchTemplate;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es.Range;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es.Term;
import com.mogudiandian.elasticsearch.orm.core.request.api.complex.ComplexSearchRequest;
import com.mogudiandian.elasticsearch.orm.core.request.api.complex.ConditionExpression;
import com.mogudiandian.elasticsearch.orm.core.request.api.complex.Operator;
import com.mogudiandian.elasticsearch.orm.core.request.api.simple.SimpleSearchRequest;
import com.mogudiandian.elasticsearch.orm.core.request.sql.SqlSearchRequest;
import com.mogudiandian.elasticsearch.orm.core.response.MatchRecord;
import com.mogudiandian.elasticsearch.orm.core.response.SearchResult;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class Test {

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

    public static void main(String[] args) {
    }

    private static void search(Long userId, String keyword, Date time1, Date time2) {

        // 简单方式
        // 优点 使用简单
        // 缺点 只能 a and b and c 只能进行等值查询(= 和 in) 并且需要在字符串中写字段名(不好refactor) 不支持嵌套 不支持模糊搜索
        {
            SimpleSearchRequest searchRequest = new SimpleSearchRequest(UserEntity.class);
            searchRequest.setKeyword(keyword);
            if (time1 != null || time2 != null) {
                searchRequest.setLifecycleRange(new LifecycleRange(time1, time2));
            }
            searchRequest.appendQueryField("userId", userId);
        }

        // 复杂方式
        // 优点 可以进行非等值的查询(> < between like) 可以做复杂条件例如 a and b and (c or (d and e)) 支持嵌套查询 支持模糊搜索
        // 缺点 需要在字符串中写字段名(不好refactor)
        {
            ComplexSearchRequest searchRequest = new ComplexSearchRequest(UserEntity.class);
            searchRequest.setKeyword(keyword);
            if (time1 != null || time2 != null) {
                searchRequest.setLifecycleRange(new LifecycleRange(time1, time2));
            }
            if (userId != null) {
                searchRequest.appendExpression(new ConditionExpression("userId", Operator.EQUAL, userId));
            }
        }

        // 注解方式
        // 优点 使用类和注解做参数 使用方便 并且在类中refactor非常容易 支持模糊搜索
        // 缺点 只能 a and b and c 只能进行等值查询(= 和 in) 需要写查询类相对麻烦 不支持嵌套查询
        {
            UserSearchDTO userSearchDTO = new UserSearchDTO();
            userSearchDTO.setStart(time1 != null ? time1.getTime() : null);
            userSearchDTO.setEnd(time2 != null ? time2.getTime() : null);
            userSearchDTO.setUserId(userId);
            AnnotatedSearchRequest searchRequest = new AnnotatedSearchRequest(UserEntity.class, userSearchDTO);
            searchRequest.setKeyword(keyword);
        }

        // SQL方式
        // 优点 可以进行ES支持的任意查询 学习成本低 即使不了解ES也能使用 支持模糊搜索
        // 缺点 不支持转义字符 执行效率相对低(要parse and explain) 对于null处理不好(语义不明确) 对于日期类型处理不好(只能按固定格式)
        {
            String sql = "where (nick like ? or description like ?) and receivedTime between ? and ? order by receivedTime desc";
            SqlSearchRequest searchRequest = new SqlSearchRequest(UserEntity.class, sql);
            searchRequest.setParameters(keyword, keyword, time1 != null ? time1.getTime() : null, time2 != null ? time2.getTime() : null);
        }

        // 下面是调用
        {
            ElasticsearchOrmClient client = new DefaultElasticsearchOrmClient();
            SearchRequest searchRequest = new SimpleSearchRequest(null);
            SearchResult<UserEntity> searchResult = client.search(searchRequest);
            List<MatchRecord<UserEntity>> list = searchResult.getList();
            List<UserEntity> entities = list.stream()
                                            .map(MatchRecord::getEntity)
                                            .collect(Collectors.toList());
        }
    }

}
