package demo;

// lombok autogenerates getters, setters, toString() and a builder (see https://projectlombok.org/):

public class Greetings {
    private long timestamp;
    private String message;
    
    public Greetings(){
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    @Override
    public String toString()
    {
        return "Greetings [timestamp=" + timestamp + ", message=" + message + "]";
    }
    
    
    
}
