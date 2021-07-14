package cn.rapidtsdb.tsdb.meta;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * READ ONLY CLASS
 */
@Data
@AllArgsConstructor
public class TagKV {
    private final String tagK;
    private final String tagV;

    public static List<TagKV> fromMap(Map<String, String> tags) {
        if (tags == null) {
            return null;
        }
        if (tags.size() == 0) {
            return new ArrayList<>();
        }
        List<TagKV> tagKVList = new ArrayList<>();
        tags.forEach((k, v) -> tagKVList.add(new TagKV(k, v)));
        return tagKVList;
    }
}
