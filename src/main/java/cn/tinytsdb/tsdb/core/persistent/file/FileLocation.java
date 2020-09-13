package cn.tinytsdb.tsdb.core.persistent.file;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileLocation {
    private String dir;
    private String filename;

    public String getPathWithFilename() {
        if(dir.endsWith("/")) {
            return dir+filename;
        } else {
            return dir+"/"+filename;
        }
    }

    @Override
    public String toString() {
        return "FileLocation{" + "dir='" + dir + '\'' + ", filename='" + filename + '\'' + '}';
    }
}
