#include<Stdio.h>

void triangle(int n){
    int val, x, y;
    for(x=0;x<n;x++){
        for(y=0;y<n-x-1;y++){
            printf(" "); }
		 val= 1;
        for(y = 0; y <= x; y++) {
            printf("%d ", val);
            val = val * (x - y) / (y + 1); }
        printf("\n");   }
}
int main() {
    int n;
    printf("Enter the number of rows: ");
    scanf("%d", &n);
    triangle(n);
    return 0;
}


