#include "consistent_hashing.h"
#include <iostream>
extern GlobalController *gc;
extern DataBase *db;
int main(void) {
    // Initialize global controller.
    if (gc == nullptr) {
        gc = new GlobalController("servers.txt");
    }

    // intialize data source.
    if (db == nullptr) {
        db = new DataBase();
    }

    // Writting and getting value from server 10.0.1.6:11211
    gc->Put("one", "hello");
    std::cout << gc->Get("one") << "\n";

    // Writting and getting value from server 10.0.1.7:11211
    gc->Put("two", "world");
    std::cout << gc->Get("two") << "\n";

    // scenario server 10.0.1.6:11211 breakdown.
    gc->DelServer("10.0.1.6:11211");

    // Now, for same key "one", we get value from 10.0.1.1:65535 instead of 10.0.1.6:11211
    std::cout << gc->Get("one") << "\n";

    // Adding server 10.0.1.10:12333
    gc->addServer("10.0.1.10:12333");

    // Writting and getting value from server 10.0.1.5:11211
    gc->Put("three", "lucky");
    

    delete gc;
    delete db;
    return 0;
}