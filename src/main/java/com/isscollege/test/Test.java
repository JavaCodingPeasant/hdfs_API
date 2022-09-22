package com.isscollege.test;

import com.isscollege.utils.HdfsUtil;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * 作者：杜丹东
 * 日期：2022/1/21 21:30
 */
public class Test {
    private static HdfsUtil hdfsUtil =null;
    @Before
    public void init(){
        hdfsUtil = new HdfsUtil();
    }

    @org.junit.Test
    public void test() throws Exception {
        FileSystem fs = hdfsUtil.getFileSystem();
//        long used = fs.getUsed();//文件系统总大小
        long used = fs.getUsed(new Path("/mmm"));//文件系统总大小
        System.out.println(used);
        FSDataInputStream fsis = fs.open(new Path("/mmm/1.jpg"));
        FileOutputStream fos = new FileOutputStream("C:/Users/Administrator/Desktop/3.jpg");
        IOUtils.copyBytes(fsis,fos,4096);

    }


    //获取指定目录下的文件或文件
    @org.junit.Test
    public void getAllFilesOrDirc() throws Exception {
        FileSystem fs = hdfsUtil.getFileSystem();
        String target="/hadoopDemo"; //写死的目标路径，项目中应该传过来，做项目时也不应该查询根目录
        FileStatus[] fileStatuses = fs.listStatus(new Path(target));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus);
            System.out.println(fileStatus.getPath().getName());
        }
        Assert.assertNotEquals(null,fileStatuses);
    }
    //创建文件夹
    @org.junit.Test
    public void addDir() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
        String target="/hadoopDemo/demo"; //写死的目标路径，项目中应该传过来
        boolean exists = fs.exists(new Path(target)); //判断是否存在指定路径,可以少一次与hadoop的交互
        System.out.println(exists);
        if(!exists) {
            boolean flag = fs.mkdirs(new Path(target));//没有就创建，有就不创建
            Assert.assertEquals(true,flag);
        }else{
            System.out.println("没有创建");
        }
//        String target="/hadoopDemo2/demo"; //创建多级路径
//        boolean flag = fs.mkdirs(new Path(target));// 返回true，说明可以创建多级文件夹
//        System.out.println(flag);
    }
    //上传文件
    @org.junit.Test
    public void addFile() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
        Path src=new Path("G:\\music\\视频\\opera2.mp4");
//        Path target=new Path("/hadoopDemo/demo2"); //hadoop的文件路径
        Path target=new Path("/ddd"); //hadoop的文件路径
        //此方法没有返回值，可以测试引起变化的内容,hadoop的路径不存在会自动创建，但不会执行上传操作
        //通过此方法创建的文件夹，再次执行上传操作也不会成功，所以不建议使用
        fs.copyFromLocalFile(src,target);
        Path test=new Path("/hadoopDemo/demo2/2.jpg");
        boolean exists = fs.exists(test);
        String result=exists?"存在":"不存在";
        System.out.println(result);
        Assert.assertEquals(true,exists);
    }
    //删除
    @org.junit.Test
    public void delete() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
        Path target=new Path("/mmm");
        Path target2=new Path("/hadoopDemo");
        //为true时，当参数为目录时，会递归删除此目录下的所有文件，谨慎操作
        //参数为具体的文件路径时会只删除目录下的指定文件
        boolean flag = fs.delete(target, true);
        Assert.assertEquals(true,flag);
    }
    //修改
    @org.junit.Test
    public void modify() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
//        Path src=new Path("/hadoopDemo/demo/2.jpg");
//        Path target=new Path("/hadoopDemo/demo/3.jpg");
//        Path src=new Path("/hadoopDemo/demo");
//        Path target=new Path("/hadoopDemo/demo2");
        Path src=new Path("/hadoopDemo2");
        Path target=new Path("/hadoopDemo");
        boolean flag = fs.rename(src,target);
        Assert.assertEquals(true,flag);
    }

    //    下载
    @org.junit.Test
    public void download() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
        Path src=new Path("/hadoopDemo/demo2/2.jpg");
        boolean flag = fs.exists(src);
        if(flag){
            Path target=new Path("C:/Users/Administrator/Desktop/");
    //        fs.copyToLocalFile(src,target);//此方法无返回值,可以用
    //        第一个参数为true时，下载完成时会删除服务器的路径，推荐使用（为false）
    //        fs.copyToLocalFile(true,src,target);
            //路径为目录时，会递归将此目录下的所有问价你都下载下来
            fs.copyToLocalFile(false,new Path("/hadoopDemo"),target);
        }
        boolean flag2 = fs.exists(src);
        Assert.assertEquals(false,flag2);
    }
    //查看文档在集群中的位置
    @org.junit.Test
    public void getFileLoc() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
        String target="/hadoopDemo/1.jpg"; //写死的目标路径，项目中应该传过来，做项目时也不应该查询根目录
        FileStatus fileStatus = fs.getFileStatus(new Path(target));
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (int i = 0; i < fileBlockLocations.length; i++) {
            String[] hosts = fileBlockLocations[i].getHosts();
            System.out.println("block"+i+"-location:"+hosts[0]);
        }
    }

    @org.junit.Test
    public void createFile() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
        String target="/hadoopDemo/test.txt";
        Path path = new Path(target);
        //第二个参数为是否覆盖，参数为false时，如果文件已经存在将会报错
        FSDataOutputStream os = fs.create(path, true);
        String content="你好啊,hadoop";
//        os.writeChars(content);//中文会乱码
//        os.writeUTF(content); //方式1
        byte[] b = content.getBytes();
        os.write(b,0,b.length);//方式2
        os.close();
    }


    @org.junit.Test
    public void readFileContent() throws Exception{
        FileSystem fs = hdfsUtil.getFileSystem();
        Path path = new Path("/hadoopDemo/test.txt");
        FSDataInputStream is = fs.open(path);
        //方式1
//        byte[] b = new byte[16];//缓冲区设置太大，会有很多乱码似的占位符
//        int len=0;
//        while((len=is.read(b))!=-1){
//            String s = new String(b);
//            System.out.println(s);
//        }
        //方式2
        InputStream ws = is.getWrappedStream();//通过文件系统的数据流获取一个包装流
        BufferedReader br = new BufferedReader(new InputStreamReader(ws));
        String s = br.readLine();
        System.out.println(s);
        br.close();
        ws.close();
        is.close();
    }

    //合并文件
    @org.junit.Test
    public void mergeFiles() throws Exception{ //不支持往文件中添加内容
        FileSystem fs = hdfsUtil.getFileSystem();
        Path path = new Path("/hadoopDemo/test.txt");
        FileStatus[] fileStatuses = fs.listStatus(path);
        System.out.println(fileStatuses[0].getLen());//16
        FSDataInputStream is = fs.open(path);
        FSDataOutputStream os = fs.append(path);
        IOUtils.copyBytes(is,os,16);
    }

    @After
    public void destroy(){
        hdfsUtil.closeHdfs();
    }
}
