package org.opencredo.mesos;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

public class CalculatePiExecutor implements Executor {

	@Override
	public void registered(ExecutorDriver executorDriver,
			Protos.ExecutorInfo executorInfo,
			Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {

		System.out.println("This little fella framework has been registered");
	}

	@Override
	public void reregistered(ExecutorDriver executorDriver,
			Protos.SlaveInfo slaveInfo) {
		System.out
				.println("This little fella framework has been re-registered");
	}

	@Override
	public void disconnected(ExecutorDriver executorDriver) {
		System.out.println("This little fella framework has been disconnect");
	}

	@Override
	public void launchTask(ExecutorDriver executorDriver,
			Protos.TaskInfo taskInfo) {
		System.out.println("Launching new task: "
				+ taskInfo.getTaskId().getValue());
		System.out
				.println("Task data is: " + taskInfo.getData().toStringUtf8());

		executorDriver.sendFrameworkMessage("ALRIGHT".getBytes());
		Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
				.setTaskId(taskInfo.getTaskId())
				.setState(Protos.TaskState.TASK_FINISHED).build();
		executorDriver.sendStatusUpdate(status);
	}

	@Override
	public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {

	}

	@Override
	public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {

	}

	@Override
	public void shutdown(ExecutorDriver executorDriver) {

	}

	@Override
	public void error(ExecutorDriver executorDriver, String s) {

	}

	public static void main(String[] args) throws Exception {
		MesosExecutorDriver driver = new MesosExecutorDriver(
				new CalculatePiExecutor());
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}