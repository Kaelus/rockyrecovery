package rockyrecovery;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;

import rocky.ctrl.RockyController;
import rocky.ctrl.RockyController.RockyControllerRoleType;
import rocky.ctrl.cloud.ValueStorageDynamoDB;
import rockyrecovery.Coordinator.NoCloudFailureRecoveryWorker;
import rocky.ctrl.RockyStorage;
import rocky.ctrl.Storage;
import rocky.ctrl.ValueStorageLevelDB;

public class CoordinatorTest {

	@Test
	public void testNoCloudFailureRecoveryEpochEa3Simple() throws ExecutionException, InterruptedException {
		System.out.println("entered testNoCloudFailureRecovery");
		int numEpoch = 3;
		int numBlockUsed = 3;
		byte[] bytesWriteBuffer = new byte[512];
		byte[] bytesReadBuffer = new byte[512];
		byte[] bytesExpected = new byte[512];
		byte[] bytesToWrite;
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		System.out.println("Rocky Storage being instantiated..");
		Storage storage = new RockyStorage("testing");
		RockyStorage.debugPrintoutFlag = true;
		RockyController.role = RockyControllerRoleType.Owner;
		storage.connect();
		String[][] valueList = {{"b0e1", "b1e1", "b2e1"},
								{"b0e2", "b1e2", "b2e2"},
								{"b0e3", "b1e3", "b2e3"}};
		String[] coherentDiskImage = {"b0e2", "b1e2", "b2e2"};
		System.out.println("Rocky write..");
		for (int i = 0; i < numEpoch; i++) {
			for (int j = 0; j < numBlockUsed; j++) {
				if (valueList[i][j] != null) {
					bytesToWrite = valueList[i][j].getBytes();
					System.arraycopy(bytesToWrite, 0, bytesWriteBuffer, 0, bytesToWrite.length);
					System.out.println("i=" + i + " j=" + j + " writing Str=" + new String(bytesWriteBuffer, Charsets.UTF_8));
					storage.write(bytesWriteBuffer, j*RockyStorage.blockSize);
					Thread.sleep(100);
					storage.read(bytesReadBuffer, j*RockyStorage.blockSize);
					System.out.println("reading block ID=" + j + " read Str=" + new String(bytesReadBuffer, Charsets.UTF_8));
				}
			}
			storage.flush();
		}
		Thread.sleep(100);
		for (int i = 0; i < numBlockUsed; i++) {
			storage.read(bytesReadBuffer, i * RockyStorage.blockSize);
			System.out.println("Write confirmation. Assert for block ID=" + i);
			System.arraycopy(valueList[numEpoch-1][i].getBytes(), 0, bytesExpected, 0, valueList[numEpoch-1][i].getBytes().length);
			System.out.println("expectedStr=" + new String(bytesExpected, Charsets.UTF_8));
			System.out.println("actualStr=" + new String(bytesReadBuffer, Charsets.UTF_8));
			Assert.assertArrayEquals(bytesExpected, bytesReadBuffer);
		}
		System.out.println("Rocky disconnect..");
		storage.disconnect();
//		try {
//			((ValueStorageLevelDB)RockyStorage.localEpochBitmaps).finish();
//			((ValueStorageLevelDB)RockyStorage.localBlockSnapshotStore).finish();
//			((ValueStorageLevelDB)RockyStorage.versionMap).finish();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		Coordinator.initialize();
		Coordinator.epochEa = Coordinator.epochEa + 3;
		Coordinator.epochEp = Coordinator.epochEp + 3;
		System.out.println("Rocky Coordinator being instantiated..");
		Coordinator.cloudEpochBitmaps = new ValueStorageDynamoDB(Coordinator.cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
		Coordinator.cloudBlockSnapshotStore = new ValueStorageDynamoDB(Coordinator.cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
		Coordinator.localEpochBitmaps = RockyStorage.localEpochBitmaps;
		Coordinator.localBlockSnapshotStore = RockyStorage.localBlockSnapshotStore;
		Coordinator.versionMap = RockyStorage.versionMap;
		Coordinator server = new Coordinator(Coordinator.myID);
		server.startNoCloudFailureRecoveryWorker();
		Coordinator.noCloudFailureRecoveryFlag = true;
		Thread.sleep(50); // wait for recovery procedure to finish
		RockyStorage.presenceBitmap.set(0, numBlockUsed);
//		try {
//			RockyStorage.localEpochBitmaps = new ValueStorageLevelDB("localEpochBitmapsTable");
//			RockyStorage.localBlockSnapshotStore = new ValueStorageLevelDB("localBlockSnapshotStoreTable");
//			RockyStorage.versionMap = new ValueStorageLevelDB("versionMapTable");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		storage.connect();
		System.out.println("Do checking now..");
		for (int i = 0; i < numBlockUsed; i++) {
			storage.read(bytesReadBuffer, i*RockyStorage.blockSize);
			System.arraycopy(coherentDiskImage[i].getBytes(), 0, bytesExpected, 0, valueList[numEpoch-1][i].getBytes().length);
			System.out.println("Assert for block ID=" + i);
			Assert.assertArrayEquals(bytesExpected, bytesReadBuffer);
		}
		System.out.println("Finishing testNoCloudFailureRecoveryEpochEa3Simple");		
	}
	
}
