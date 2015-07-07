#include <math.h>
#include <float.h>
#include "wkbtest.h"

#define WKBZOFFSET 0x80000000
#define WKBMOFFSET 0x40000000


//  DO NOT USE THESE decoding function (except for debugging purposes)
//    The code is NOT maintained and is thrown together


// unsupported debugging function.
// given a wkb input thats a geometrycollection, returns its size and prints out
// its contents
//
//  Its really messy - dont even think about using this for anything
//
// you shouldnt call this function; just call decode_wkb() and it will
// call this function

//#define DEBUG 1




void decode_wkb_collection(char *wkb,int	*size);

void swap_char(char	*a,char *b)
{
	char c;

	c = *a;
	*a=*b;
	*b=c;
}


void	flip_endian_double(char 	*d)
{
	swap_char(d+7, d);
	swap_char(d+6, d+1);
	swap_char(d+5, d+2);
	swap_char(d+4, d+3);
}

void		flip_endian_int32(char		*i)
{
	swap_char (i+3,i);
	swap_char (i+2,i+1);
}


void decode_wkb_collection(char *wkb,int	*size)
{
	int	offset =0;
	bool	flipbytes;
	int	total_size=0,sub_size;
	int	numb_sub,t;
	bool	first_one = TRUE;

	if (wkb[0] ==0 )  //big endian
	{
		if (BYTE_ORDER == LITTLE_ENDIAN)
			flipbytes= 1;
		else
			flipbytes= 0;
	}
	else //little
	{
		if (BYTE_ORDER == LITTLE_ENDIAN)
			flipbytes= 0;
		else
			flipbytes= 1;
	}

	memcpy(&numb_sub, wkb+5,4);
	if (flipbytes)
		flip_endian_int32( (char *) & numb_sub) ;

	printf("GEOMETRYCOLLECTION(\n");
	offset = 9;
	for (t=0; t<numb_sub; t++)
	{
		if (first_one)
		{
			first_one = FALSE;
		}
		else
		{
			printf(",");
		}

		printf("	");
		decode_wkb( wkb + offset, &sub_size);
		total_size += sub_size;
		offset += sub_size;
	}
	*size = total_size +9 ;
	printf(")\n");
	return;
}

//dump wkb to screen (for debugging)
// assume endian is constant though out structure
void decode_wkb(char *wkb, int *size)
{

	bool	flipbytes;
	uint32	type;
	uint32	n1,n2,n3,t,u,v;
	bool	is3d;
	bool	first_one,first_one2,first_one3;

	int	offset,offset1;
	double	x,y,z;
	int	total_points;



#if DEBUG
	printf("decoding wkb\n");
#endif


	if (wkb[0] ==0 )  //big endian
	{
		if (BYTE_ORDER == LITTLE_ENDIAN)
			flipbytes= 1;
		else
			flipbytes= 0;
	}
	else //little
	{
		if (BYTE_ORDER == LITTLE_ENDIAN)
			flipbytes= 0;
		else
			flipbytes= 1;
	}

#if DEBUG
	printf("	+ flipbytes = %i\n", flipbytes);
#endif

#if DEBUG
	printf("info about wkb:\n");
#endif
	memcpy(&type, wkb+1,4);
	if (flipbytes)
		flip_endian_int32( (char *) & type) ;

	is3d = 0;

	if (type&WKBZOFFSET)
	{
		is3d = 1;
		type = type&(~WKBZOFFSET);
	}
#if DEBUG
	printf("	Type = %i (is3d = %i)\n", type, is3d);
#endif
	if (type == 1)  //POINT()
	{
		printf("POINT(");
		if (is3d)
		{
			memcpy(&x, &wkb[5], 8);
			memcpy(&y, &wkb[5+8], 8);
			memcpy(&z, &wkb[5+16], 8);
			if (flipbytes)
			{
				flip_endian_double( (char *) & x) ;
				flip_endian_double( (char *) & y) ;
				flip_endian_double( (char *) & z) ;
			}
			printf("%g %g %g)",x,y,z );
		}
		else
		{
			memcpy(&x, &wkb[5], 8);
			memcpy(&y, &wkb[5+8], 8);
			if (flipbytes)
			{
				flip_endian_double( (char *) & x) ;
				flip_endian_double( (char *) & y) ;
			}
			printf("%g %g)", x,y);
		}
		printf("\n");
		if (is3d)
			*size = 29;
		else
			*size = 21;
		return;

	}
	if (type == 2)
	{
		printf("LINESTRING(");
		memcpy(&n1, &wkb[5],4);
		if (flipbytes)
			flip_endian_int32( (char *) & n1) ;
		//	printf("  --- has %i sub points\n",n1);
		first_one = TRUE;
		for (t=0; t<n1; t++)
		{
			if (first_one)
			{
				first_one = FALSE;
			}
			else
			{
				printf(",");
			}
			if (is3d)
			{
				memcpy(&x, &wkb[9+t*24],8);
				memcpy(&y, &wkb[9+t*24+8],8);
				memcpy(&z, &wkb[9+t*24+16],8);
				if (flipbytes)
				{
					flip_endian_double( (char *) & x) ;
					flip_endian_double( (char *) & y) ;
					flip_endian_double( (char *) & z) ;
				}
				printf("%g %g %g",x,y,z);
			}
			else
			{
				memcpy(&x, &wkb[9+t*16],8);
				memcpy(&y, &wkb[9+t*16+8],8);
				if (flipbytes)
				{
					flip_endian_double( (char *) & x) ;
					flip_endian_double( (char *) & y) ;
				}
				printf("%g %g",x,y);
			}
		}


		printf(")\n");
		if (is3d)
			*size = 9 + n1*24;
		else
			*size = 9 + n1*16;
		return;
	}
	if (type == 3)
	{
		*size = 9;
		printf("POLYGON(");
		memcpy(&n1, &wkb[5],4);
		if (flipbytes)
			flip_endian_int32( (char *) & n1) ;
		//printf("  --- has %i rings\n",n1);
		*size += 4*n1;
		offset= 9;
		first_one = TRUE;
		for (u=0; u<n1; u++)
		{
			memcpy(&n2, &wkb[offset],4);
			if (flipbytes)
				flip_endian_int32( (char *) & n2) ;
			//	printf(" ring %i: has %i points\n",u,n2);


			if (first_one)
			{
				first_one = FALSE;
			}
			else
			{
				printf(",");
			}
			printf("(");

			first_one2 = TRUE;
			for (v=0; v< n2; v++)
			{
				if (first_one2)
				{
					first_one2 = FALSE;
				}
				else
				{
					printf(",");
				}
				if (is3d)
				{
					memcpy(&x, &wkb[offset + 4+ v*24],8);
					memcpy(&y, &wkb[offset + 4+ v*24 + 8],8);
					memcpy(&z, &wkb[offset + 4+ v*24 + 16],8);
					if (flipbytes)
					{
						flip_endian_double( (char *) & x) ;
						flip_endian_double( (char *) & y) ;
						flip_endian_double( (char *) & z) ;
					}
					printf("%g %g %g",x,y,z);

				}
				else
				{
					memcpy(&x, &wkb[offset +4 +v*16],8);
					memcpy(&y, &wkb[offset +4 +v*16 + 8],8);
					if (flipbytes)
					{
						flip_endian_double( (char *) & x) ;
						flip_endian_double( (char *) & y) ;
					}
					printf("%g %g",x,y);

				}
			}
			if (is3d)
			{
				offset +=4 +24*n2;
				*size += n2*24;
			}
			else
			{
				offset += 4+ 16*n2;
				*size += n2*16;
			}
			printf(")");
		}

		printf(")\n");

		return;

	}
	if (type == 4)
	{
		printf("MULTIPOINT(");
		memcpy(&n1,&wkb[5],4);
		if (flipbytes)
			flip_endian_int32( (char *) & n1) ;
		//	printf(" -- has %i points\n",n1);
		if (is3d)
			*size = 9 + n1*29;
		else
			*size = 9 + n1*21;
		first_one = TRUE;
		for (t=0; t<n1; t++)
		{
			if (first_one)
			{
				first_one= FALSE;
			}
			else
			{
				printf(",");
			}
			if (is3d)
			{
				memcpy(&x, &wkb[9+t*29+5],8);
				memcpy(&y, &wkb[9+t*29+8+5],8);
				memcpy(&z, &wkb[9+t*29+16+5],8);
				if (flipbytes)
				{
					flip_endian_double( (char *) & x) ;
					flip_endian_double( (char *) & y) ;
					flip_endian_double( (char *) & z) ;
				}
				printf("%g %g %g",x,y,z);
			}
			else
			{
				memcpy(&x, &wkb[9+t*21+5],8);
				memcpy(&y, &wkb[9+t*21+8+5],8);
				if (flipbytes)
				{
					flip_endian_double( (char *) & x) ;
					flip_endian_double( (char *) & y) ;
				}
				printf("%g %g",x,y);
			}
		}
		printf (")\n");
		return;
	}
	if (type == 5)
	{
		*size = 9;
		printf("MULTILINESTRING(");
		memcpy(&n2,&wkb[5],4);
		if (flipbytes)
			flip_endian_int32( (char *) & n2) ;
		//	printf(" -- has %i sub-lines\n",n2);
		*size += 9 *n2;
		offset =9;
		first_one2 = TRUE;
		for (u=0; u<n2; u++)
		{
			if (first_one2)
			{
				first_one2= FALSE;
			}
			else
			{
				printf(",");
			}
			printf("(");
			memcpy(&n1, &wkb[5 +offset],4);
			if (flipbytes)
				flip_endian_int32( (char *) & n1) ;
			//	printf("  --- has %i sub points\n",n1);
			first_one = TRUE;
			for (t=0; t<n1; t++)
			{
				if (first_one)
				{
					first_one= FALSE;
				}
				else
				{
					printf(",");
				}

				if (is3d)
				{
					memcpy(&x, &wkb[offset+9+t*24],8);
					memcpy(&y, &wkb[offset+9+t*24+8],8);
					memcpy(&z, &wkb[offset+9+t*24+16],8);
					if (flipbytes)
					{
						flip_endian_double( (char *) & x) ;
						flip_endian_double( (char *) & y) ;
						flip_endian_double( (char *) & z) ;
					}
					printf("%g %g %g",x,y,z);
				}
				else
				{
					memcpy(&x, &wkb[offset+9+t*16],8);
					memcpy(&y, &wkb[offset+9+t*16+8],8);
					if (flipbytes)
					{
						flip_endian_double( (char *) & x) ;
						flip_endian_double( (char *) & y) ;
					}
					printf("%g %g",x,y);
				}
			}
			printf(")");
			if (is3d)
			{
				*size += (24*n1);
				offset += 9 + (24*n1);
			}
			else
			{
				*size += (16*n1);
				offset += 9 + (16*n1);
			}
		}
		printf(")\n");
		return;

	}
	if (type == 6)
	{
		*size = 9;
		printf("MULTIPOLYGON(");
		memcpy(&n3,&wkb[5],4);
		if (flipbytes)
			flip_endian_int32( (char *) & n3) ;
		//printf(" -- has %i sub-poly\n",n3);
		*size += 9*n3;
		offset1 =9;//where polygon starts
		first_one3= TRUE;
		for (t=0; t<n3; t++) //for each polygon
		{
			if (first_one3)
			{
				first_one3= FALSE;
			}
			else
			{
				printf(",");
			}
			printf("(");
			//printf("polygon #%i\n",t);
			total_points = 0;
			memcpy(&n1,&wkb[offset1+5],4); //# rings
			*size += 4*n1;
			if (flipbytes)
				flip_endian_int32( (char *) & n1) ;
			//printf("This polygon has %i rings\n",n1);
			offset = offset1+9; //where linear rings are
			first_one = TRUE;
			for (u=0; u<n1; u++) //for each ring
			{
				if (first_one)
				{
					first_one= FALSE;
				}
				else
				{
					printf(",");
				}
				printf("(");
				memcpy(&n2, &wkb[offset],4);
				if (flipbytes)
					flip_endian_int32( (char *) & n2) ; //pts in linear ring
				//	printf(" ring %i: has %i points\n",u,n2);
				total_points += n2;
				first_one2 = TRUE;
				for (v=0; v< n2; v++)	//for each point
				{
					if (first_one2)
					{
						first_one2= FALSE;
					}
					else
					{
						printf(",");
					}
					if (is3d)
					{
						memcpy(&x, &wkb[offset + 4+ v*24],8);
						memcpy(&y, &wkb[offset + 4+ v*24 + 8],8);
						memcpy(&z, &wkb[offset + 4+ v*24 + 16],8);
						if (flipbytes)
						{
							flip_endian_double( (char *) & x) ;
							flip_endian_double( (char *) & y) ;
							flip_endian_double( (char *) & z) ;
						}
						printf("%g %g %g",x,y,z);
					}
					else
					{
						memcpy(&x, &wkb[offset +4 +v*16],8);
						memcpy(&y, &wkb[offset +4 +v*16 + 8],8);
						if (flipbytes)
						{
							flip_endian_double( (char *) & x) ;
							flip_endian_double( (char *) & y) ;
						}
						printf("%g %g",x,y);
					}
				}
				if (is3d)
				{
					*size += 24*n2;
					offset += 4+ 24*n2;
				}
				else
				{
					*size += 16*n2;
					offset += 4+ 16*n2;
				}
				printf(")");
			}
			printf(")");
			if (is3d)
				offset1 +=9 +24*total_points +4*n1;
			else
				offset1 += 9+ 16*total_points +4*n1;
		}
		printf(")\n");
		return;
	}
	if (type == 7)
	{
		return	decode_wkb_collection(wkb, size);
	}




}


