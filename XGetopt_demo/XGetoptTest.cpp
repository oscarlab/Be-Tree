// XGetoptTest.cpp : Defines the class behaviors for the application.
//

#include "stdafx.h"
#include "XGetoptTest.h"
#include "XGetoptTestDlg.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CXGetoptTestApp

BEGIN_MESSAGE_MAP(CXGetoptTestApp, CWinApp)
	//{{AFX_MSG_MAP(CXGetoptTestApp)
	//}}AFX_MSG
	ON_COMMAND(ID_HELP, CWinApp::OnHelp)
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CXGetoptTestApp construction

CXGetoptTestApp::CXGetoptTestApp()
{
}

/////////////////////////////////////////////////////////////////////////////
// The one and only CXGetoptTestApp object

CXGetoptTestApp theApp;

/////////////////////////////////////////////////////////////////////////////
// CXGetoptTestApp initialization

BOOL CXGetoptTestApp::InitInstance()
{
#ifdef _AFXDLL
	Enable3dControls();			// Call this when using MFC in a shared DLL
#else
	Enable3dControlsStatic();	// Call this when linking to MFC statically
#endif

	CXGetoptTestDlg dlg;
	m_pMainWnd = &dlg;
	dlg.DoModal();
	return FALSE;
}
