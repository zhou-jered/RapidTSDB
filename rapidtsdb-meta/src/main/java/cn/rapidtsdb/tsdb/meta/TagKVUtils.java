package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TagKVUtils {

    public static List<String> getAllTagKeys(List<TagKV> tagKVList) {
        Set<String> tagKeySet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(tagKVList)) {
            tagKVList.forEach(tagKV -> tagKeySet.add(tagKV.getTagK()));
        }
        return new ArrayList<>(tagKeySet);
    }

}
