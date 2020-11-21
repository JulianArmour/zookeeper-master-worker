// You should not have to do anything in this directory.
import java.io.Serializable; // we need to be able to serialize the task to save it to ZK.
// Need to be implemented by as task that needs to be sent to the dist. platform.
public interface DistTask extends Serializable
{
	// This is the function that will be invoked by the workers of the dist platform.
	public void compute();
}
