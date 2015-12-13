// XGetoptTest.h : main header file for the XGETOPTTEST application
//

#ifndef XGETOPTTEST_H
#define XGETOPTTEST_H

#ifndef __AFXWIN_H__
	#error include 'stdafx.h' before including this file for PCH
#endif

#include "resource.h"		// main symbols

/////////////////////////////////////////////////////////////////////////////
// CXGetoptTestApp:
// See XGetoptTest.cpp for the implementation of this class
//

class CXGetoptTestApp : public CWinApp
{
public:
	CXGetoptTestApp();

// Overrides
	// ClassWizard generated virtual function overrides
	//{{AFX_VIRTUAL(CXGetoptTestApp)
public:
	virtual BOOL InitInstance();
	//}}AFX_VIRTUAL

// Implementation

	//{{AFX_MSG(CXGetoptTestApp)
	//}}AFX_MSG
	DECLARE_MESSAGE_MAP()
};

/////////////////////////////////////////////////////////////////////////////

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif //XGETOPTTEST_H
