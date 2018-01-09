package custom_serializer;

public class Customer {

    private int customerID;
    private String customerName;

    public Customer(int ID, String name) {
        this.customerID = ID;
        this.customerName = name;
    }

    public int getID() {
        return customerID;
    }

    public String getCustomerName() {
        return customerName;
    }
}


