//package com.JadePenG;
//
//import net.neoremind.sshxcute.core.ConnBean;
//import net.neoremind.sshxcute.core.SSHExec;
//import net.neoremind.sshxcute.exception.TaskExecFailException;
//import net.neoremind.sshxcute.task.impl.ExecCommand;
//
///**
// * 需求描述：在实际工作中，总会有些时候需要我们通过java代码通过远程连接去linux服务器上面执行一些shell命令，包括一些集群的状态管理，执行任务，集群的可视化界面操作等等，所以我们可以通过java代码来执行linux服务器的shell命令
// * 为了解决上述问题，google公司给提出了对应的解决方案，开源出来了一个jar包叫做sshxcute，通过这个jar包我们可以通过java代码，非常便捷的操作我们的linux服务器了
// *
// */
//public class SshExecTest {
//
//
//    public static void main(String[] args) throws TaskExecFailException {
//
//        ConnBean connBean = new ConnBean("node03", "root", "hadoop");
//
//        //获取client
//        SSHExec instance = SSHExec.getInstance(connBean);
//
//        //启动client
//        instance.connect();
//
//        //执行操作(操作linux服务器)
//        ExecCommand execCommand = new ExecCommand("echo testSSHExec > /export/servers/testsshexec.txt");
//
//        instance.exec(execCommand);
//
//        //关闭client
//        instance.disconnect();
//
//
//    }
//
//
//}
